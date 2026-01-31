import re
from typing import Any, Dict, Optional, Tuple

from lark import Lark, Transformer, v_args


DEFAULT_GRAMMAR = r"""
    pair: KEY DELIMITER [VALUE]
    KEY: /[a-zA-Z_]\w*/
    DELIMITER: /\s*[:=]\s*/ | /\s+/
    VALUE: /.+?(?=(?:,\s*|\s+)[a-zA-Z_]\w*\s*[:=]|$)/
    %import common.WS
    %ignore WS
"""


class LogTransformer(Transformer):
    # maps the children of the 'pair' rule directly to func args
    @v_args(inline=True)
    def pair(self, key: Any, delimiter: str, value=None) -> Tuple[str, Any]:
        """_summary_

        :param key: _description_
        :param delimiter: _description_
        :param value: _description_, defaults to None
        :return: _description_
        """
        k = str(key)
        v = "None"
        if value:
            v = str(value).rstrip(",").strip()
        return k, v


class GrammarParser:
    """_summary_"""

    def __init__(self, grammar_override: Optional[str] = None) -> None:
        """_summary_

        :param grammar_override: _description_, defaults to None
        """
        self.pair_grammar = DEFAULT_GRAMMAR
        if grammar_override:
            self.pair_grammar = grammar_override

        self.pair_parser = Lark(
            self.pair_grammar,
            start="pair",
            parser="lalr",
        )
        # create one instance of the transformer to reuse
        self.transformer = LogTransformer()

    def smart_parse(self, raw_str: str) -> Dict[str, Any]:
        """_summary_

        :param raw_str: _description_
        :return: _description_
        """
        extracted = {}
        content = re.sub(r"([a-zA-Z]+)\s+(\d+):\s*", r"\1=\2, ", raw_str)
        segments = re.split(r"(?:,\s*|\s+)(?=[a-zA-Z_]\w*\s*[:=])", content)

        for seg in segments:
            seg = seg.strip()
            if not seg:
                continue
            try:
                tree = self.pair_parser.parse(seg)
                k, v = self.transformer.transform(tree)
                extracted[k] = v
            except Exception:
                continue

        return dict(sorted(extracted.items()))
