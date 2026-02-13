use pyo3::prelude::*;
use pyo3::types::{PyDict, PyString, PyList};
use pyo3::IntoPyObject; 
use pest::Parser;
use pest_derive::Parser;
use rayon::prelude::*;
use regex::Regex;
use std::sync::OnceLock;
use serde_json::{Value, Map};

#[derive(Parser)]
#[grammar = "rust/grammar.pest"]
pub struct KVParser;

static HEADER_FIX: OnceLock<Regex> = OnceLock::new();
static SPLIT_PATTERN: OnceLock<Regex> = OnceLock::new();


// recursive function to help convert python dictionary to json
fn py_to_json_recursive(obj: &Bound<'_, PyAny>) -> Value {
    if let Ok(dict) = obj.cast::<PyDict>() {
        let mut map = Map::new();
        for (k, v) in dict {
            // preserve key order
            map.insert(k.to_string(), py_to_json_recursive(&v));
        }
        Value::Object(map)
    } else if let Ok(list) = obj.cast::<PyList>() {
        let vec: Vec<Value> = list.iter().map(|item | py_to_json_recursive(&item)).collect();
        Value::Array(vec)
    } else if let Ok(b) = obj.extract::<bool>(){
        Value::Bool(b)
    } else if let Ok(n) = obj.extract::<f64>() {
        serde_json::Number::from_f64(n).map_or(Value::Null, Value::Number)
    } else if let Ok(s) = obj.extract::<String>() {
        Value::String(s)
    } else {
        if obj.is_none() {
            Value::Null
        } else {
            Value::String(obj.to_string())
        }
    }
}


/// helper function to handle the JSON Fast Path
fn try_parse_json(trimmed: &str) -> Option<Vec<(String, String)>> {
    // quick exit if it doesn't look like JSON
    if !((trimmed.starts_with('{') && trimmed.ends_with('}')) || 
         (trimmed.starts_with('[') && trimmed.ends_with(']'))) {
        return None;
    }

    // attempt to load JSON
    let json_val: Value = serde_json::from_str(trimmed).ok()?;

    match json_val {
        Value::Object(map) => {
            let extracted = map.into_iter().map(|(k, v)| {
                let v_str = match v {
                    Value::String(s) => s,
                    _ => v.to_string(),
                };
                (k.to_lowercase(), v_str)
            }).collect();
            Some(extracted)
        },
        Value::Array(_) => {
            // treat the whole array as a single entry to keep KV structure
            Some(vec![("json_data".to_string(), trimmed.to_string())])
        },
        _ => None,
    }
}

enum InputType {
    Raw(String),
    AlreadyParsed(Value, Py<PyDict>),
}


#[pyfunction]
pub fn smart_parse_batch(py: Python<'_>, logs: Vec<Py<PyAny>>) -> PyResult<Vec<Py<PyAny>>> {
    let header_fix = HEADER_FIX.get_or_init(|| {
        Regex::new(r"(?P<name>[a-zA-Z]+)\s+(?P<id>\d+):\s*").unwrap()
    });
    
    let split_pattern = SPLIT_PATTERN.get_or_init(|| {
        Regex::new(r"(?:,\s*|\s+)[a-zA-Z_]\w*\s*[:=]").unwrap()
    });

    // GIL-bound preprocessing
    let inputs: Vec<InputType> = logs.into_iter().map(|item| {
        let bound_item = item.into_bound(py);
        if let Ok(dict) = bound_item.cast::<PyDict>() {
            let json_val = py_to_json_recursive(dict.as_any());
            InputType::AlreadyParsed(json_val, dict.clone().unbind())
        } else if let Ok(py_str) = bound_item.cast::<PyString>() {
            InputType::Raw(py_str.to_string())
        } else {
            InputType::Raw(bound_item.to_string())
        }
    }).collect(); 

    // rayon parallel processing
    let processed_data: Vec<(String, Vec<(String, String)>, Vec<String>, Option<Py<PyDict>>)> = py.detach(|| {
        inputs.into_par_iter()
            .map(|input| {
                match input {
                    InputType::AlreadyParsed(rust_val, dict_handle) => {
                        // serialization (but maintain key order)
                        let json_str = serde_json::to_string(&rust_val).unwrap_or_else(|_| "{}".to_string());
                        (json_str, Vec::new(), Vec::new(), Some(dict_handle))
                    },
                    InputType::Raw(raw_str) => {
                        let trimmed = raw_str.trim();
                        // --- JSON PATH ---
                        if let Some(extracted) = try_parse_json(trimmed) {
                            return (raw_str, extracted, Vec::new(), None);
                        }
                        
                        // --- Key Value Pair PATH (fallback) ---
                        let mut extracted = Vec::new();
                        let mut unparsed_segments = Vec::new();
                        let content = header_fix.replace_all(&raw_str, "$name-$id, ").to_string();

                        let mut segments = Vec::new();
                        let mut last = 0;
                        for mat in split_pattern.find_iter(&content) {
                            segments.push(&content[last..mat.start()]);
                            let match_str = mat.as_str();
                            let key_start_offset = match_str.find(|c: char| c.is_alphanumeric() || c == '_').unwrap_or(0);
                            last = mat.start() + key_start_offset;
                        }
                        segments.push(&content[last..]);

                        for seg in segments {
                            let seg_trimmed = seg.trim();
                            if seg_trimmed.is_empty() { continue; }

                            if let Ok(mut pairs) = KVParser::parse(Rule::pair_segment, seg_trimmed) {
                                let pair = pairs.next().unwrap();
                                let mut inner = pair.into_inner();
                                let k = inner.next().unwrap().as_str().to_lowercase();
                                let _delim = inner.next().unwrap();
                                
                                let v = inner.next()
                                    .map(|val| {
                                        let s = val.as_str().trim();
                                        s.strip_suffix(',').unwrap_or(s).trim().to_string()
                                    })
                                    .filter(|s| !s.is_empty())
                                    .unwrap_or_else(|| "None".to_string());
                                
                                extracted.push((k, v));
                            } else {
                                unparsed_segments.push(seg_trimmed.to_string());
                            }
                        }
                        (raw_str, extracted, unparsed_segments, None)
                    } 
                }
            })
            .collect()
    });

    // python dict reconstruction (Back on the Main Thread/GIL)
    let mut results = Vec::with_capacity(processed_data.len());
    for (source_str, pairs, unparsed, existing_dict) in processed_data {
        let final_dict = if let Some(py_dict_ref) = existing_dict {
            py_dict_ref.into_bound(py)
        } else {
            let dict = PyDict::new(py);
            for (k, v) in pairs {
                let _ = dict.set_item(k, v);
            }
            if !unparsed.is_empty() {
                let _ = dict.set_item("_unparsed", unparsed.join(" "));
            }
            dict
        };

        let log_tuple = (source_str, final_dict).into_pyobject(py)?;
        results.push(log_tuple.into_any().unbind());
    }

    Ok(results)
}

#[pymodule]
fn rust_ingestion(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(smart_parse_batch, m)?)?;
    Ok(())
}