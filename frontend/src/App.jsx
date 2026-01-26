import React, { useState, useEffect, useRef } from 'react';
import { AnimatedMarkdown } from 'flowtoken';
// import 'flowtoken/dist/styles.css';
import './App.css';

const App = () => {
  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const scrollRef = useRef(null);

  useEffect(() => { scrollRef.current?.scrollIntoView({ behavior: 'smooth' }); }, [messages]);

  const handleSend = async (e) => {
    e.preventDefault();
    if (!input.trim() || loading) return;

    const userMsg = { role: 'user', content: input };
    // Add user message and an empty assistant shell
    setMessages(prev => [...prev, userMsg, { role: 'assistant', content: '' }]);
    setInput('');
    setLoading(true);

    // This mutable ref acts as our "accumulator" outside of the React render loop
    let chunkBuffer = ""; 

    try {
      const response = await fetch('http://localhost:8000/v1/chat/completions', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          model: 'google/gemma-3-27b-it',
          messages: [...messages, userMsg],
          stream: true
        })
      });

      const reader = response.body.pipeThrough(new TextDecoderStream()).getReader();
      
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        // value is already decoded text thanks to TextDecoderStream
        const lines = value.split('\n');
        let newTokens = "";

        for (const line of lines) {
          if (!line.startsWith('data: ') || line.includes('[DONE]')) continue;
          try {
            const data = JSON.parse(line.slice(6));
            newTokens += data.choices[0].delta.content || '';
          } catch (e) {}
        }

        if (newTokens) {
          chunkBuffer += newTokens;
          // Optimization: We only update the content of the specific last message object.
          // We use the functional update to target ONLY the last item without cloning the full history array logic repeatedly.
          setMessages(prev => {
            const lastIndex = prev.length - 1;
            const updatedLast = { ...prev[lastIndex], content: chunkBuffer };
            return [...prev.slice(0, lastIndex), updatedLast];
          });
        }
      }
    } finally {
      setLoading(false);
    }
  };

  const handleKeyDown = (e) => {
  // Submit on Enter, but allow Shift+Enter for new lines
  if (e.key === 'Enter' && !e.shiftKey) {
    e.preventDefault();
    handleSend(e);
  }
};

  return (
    <div className="chat-card">
      <div className="chat-app">
        <div className="messages-list">
          {messages.map((m, i) => (
            <div key={i} className={`message-wrapper ${m.role}`}>
              <div className="bubble">
                <AnimatedMarkdown 
                  content={m.content}
                  animation={null} 
                />
              </div>
            </div>
          ))}
          <div ref={scrollRef} />
        </div>

        <form className="input-bar" onSubmit={handleSend}>
          <textarea 
            placeholder="Message vLLM..." 
            rows="1"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
          />
          <button type="submit" disabled={loading}>↑</button>
        </form>
      </div>
    </div>
  );
}


export default App