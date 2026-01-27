const API_BASE_URL = import.meta.env.VITE_API_URL;


const fetchChatStream = async ({ userId, conversationId, model, prompt }, signal) => {
  const response = await fetch(`${API_BASE_URL}/api/chat`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    signal,
    body: JSON.stringify({ 
      userId, 
      conversationId,
      model, 
      prompt, 
      stream: 
      true 
    })
  });

  if (!response.ok) throw new Error(`HTTP Error ${response.status}`);

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  // holds partial lines between chunks
  let lineBuffer = '';

  return new ReadableStream({
    async pull(controller) {
      const { value, done } = await reader.read();

      if (done) {
        // if there's leftover text in the buffer, send it before closing
        if (lineBuffer.trim()) controller.enqueue(lineBuffer);
        controller.close();
        return;
      }

      // decode current chunk and add to buffer
      lineBuffer += decoder.decode(value, { stream: true });

      // split by newlines to process complete lines
      const lines = lineBuffer.split('\n');
      
      // keep the last (potentially incomplete) line in the buffer
      lineBuffer = lines.pop() || '';

      for (const line of lines) {
        const cleaned = line.replace(/^data: /, '').trim();
        if (!cleaned || cleaned === '[DONE]') continue;
        
        try {
          const parsed = JSON.parse(cleaned);
          const content = parsed.choices?.[0]?.delta?.content;
          if (content) controller.enqueue(content);
        } catch (e) {
          // if JSON is partial even after split, put it back in buffer
          lineBuffer = line + '\n' + lineBuffer;
        }
      }
    },
    cancel() {
      // properly kill the underlying fetch
      reader.cancel();
    }
  });
};

export default fetchChatStream;