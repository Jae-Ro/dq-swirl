import React, { useState, useEffect, useRef } from 'react';
import { AnimatedMarkdown } from 'flowtoken';
import fetchChatStream from './api/chat'; 
import { CopyIcon } from './components/CopyIcon';
import './App.css';

const App = () => {
  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const [copiedId, setCopiedId] = useState(null);
  
  const abortControllerRef = useRef(null);
  const scrollRef = useRef(null);

  useEffect(() => { 
    scrollRef.current?.scrollIntoView({ behavior: 'smooth' }); 
  }, [messages]);

  const copyToClipboard = async (text, id) => {
    try {
      await navigator.clipboard.writeText(text);
      setCopiedId(id);
      setTimeout(() => setCopiedId(null), 2000);
    } catch (err) {
      console.error('Failed to copy: ', err);
    }
  };

  const handleKeyDown = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };
  
  const handleStop = () => {
    if (abortControllerRef.current) {
      abortControllerRef.current.abort();
    }
  };

  const updateLastMessage = (content) => {
    setMessages(prev => {
      const lastIdx = prev.length - 1;
      const updated = [...prev];
      updated[lastIdx] = { ...updated[lastIdx], content };
      return updated;
    });
  };

  const handleSend = async (e) => {
    if (e) e.preventDefault();
    if (!input.trim() || loading) return;

    const currentInput = input;
    const userMsg = { id: crypto.randomUUID(), role: 'user', content: currentInput };
    const assistantId = crypto.randomUUID();
    
    setMessages(prev => [...prev, userMsg, { id: assistantId, role: 'assistant', content: '' }]);
    setInput('');
    setLoading(true);

    const controller = new AbortController();
    abortControllerRef.current = controller;

    const timeoutSignal = AbortSignal.timeout(300_000);
    const combinedSignal = AbortSignal.any([controller.signal, timeoutSignal]);

    let accumulatedContent = "";

    try {
      const stream = await fetchChatStream({
        userId: 'user_123',
        conversationId: 'conv_456',
        model: 'openai/google/gemma-3-27b-it',
        prompt: currentInput
      }, combinedSignal);

      for await (const token of stream) {
        accumulatedContent += token;
        updateLastMessage(accumulatedContent);
      }
    } catch (error) {
      if (error.name === 'TimeoutError') {
        updateLastMessage(accumulatedContent + "\n\n[ERROR]: Request timed out.");
      } else if (error.name === 'AbortError') {
        updateLastMessage(accumulatedContent + "\n\n... (Stopped)");
      } else {
        console.error('Fetch error:', error);
        updateLastMessage(accumulatedContent + "\n\n[ERROR]: Connection failed.");
      }
    } finally {
      setLoading(false);
      abortControllerRef.current = null;
    }
  };

  return (
    <div className="app-container">
      <div className="slider"></div>
      <div className="chat-card">
        <div className="chat-app">
          <div className="messages-list">
            {messages.map((m) => (
              <div key={m.id} className={`message-wrapper ${m.role}`}>
                <div className="bubble">
                  { m.content && (
                    <div className="bubble-header">
                      <button 
                        className={`copy-btn-header ${copiedId === m.id ? 'copied' : ''}`}
                        onClick={() => copyToClipboard(m.content, m.id)}
                        aria-label="Copy message"
                      >
                        {copiedId === m.id ? (
                          <span className="copied-text">✓</span>
                        ) : (
                          <CopyIcon className="copy-icon-svg" />
                        )}
                      </button>
                    </div>
                  )}
                  <div className="markdown-container">
                    <AnimatedMarkdown content={m.content} animation={null} />
                  </div>
                </div>
              </div>
            ))}
            <div ref={scrollRef} />
          </div>

          <form className="input-bar" onSubmit={handleSend}>
            <textarea
              placeholder="Enter a prompt for Swirl"
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={handleKeyDown}
            />
            
            {loading ? (
              <button type="button" className="stop-btn" onClick={handleStop}>■</button>
            ) : (
              <button type="submit" disabled={!input.trim()}>↑</button>
            )}
          </form>
          <p> Swirl can make mistakes, so please be sure to review </p>
        </div>
      </div>
      <div className="right-pillar"></div>
    </div>
  );
}

export default App;