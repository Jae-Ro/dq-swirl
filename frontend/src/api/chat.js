const API_BASE_URL = import.meta.env.VITE_API_URL;

const fetchChatStream = async ({ userId, conversationId, model, prompt }, signal) => {
  const response = await fetch(`${API_BASE_URL}/api/chat`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    signal,
    body: JSON.stringify({ userId, conversationId, model, prompt, stream: true })
  });

  if (!response.ok) throw new Error(`HTTP Error ${response.status}`);

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let lineBuffer = '';

  return new ReadableStream({
    async pull(controller) {
      try {
        const { value, done } = await reader.read();

        if (done) {
          // if there's leftover text in the buffer, send it before closing
          if (lineBuffer.trim()) {
            controller.enqueue(lineBuffer);
          }
          controller.close();
          return;
        }

        // decode current chunk and add to buffer
        lineBuffer += decoder.decode(value, { stream: true });
        // split by newlines to process complete lines
        const lines = lineBuffer.split('\n');
        // keep the last (potentially incomplete) line in the buffer
        // this tends to be the line with partial JSON
        lineBuffer = lines.pop() || '';

        for (const line of lines) {
          const cleaned = line.replace(/^data: /, '').trim();
          if (!cleaned || cleaned === '[DONE]') continue;

          try {
            const parsed = JSON.parse(cleaned);

            // either {"data": {"content": "..."}}
            // or {"error": "..."}
            if (parsed.error) {
              controller.error(new Error(parsed.error));
              return;
            }

            const content = parsed.data?.content;
            if (content) {
              controller.enqueue(content);
            }
          } catch (e) {
            // If JSON is malformed or partial, put it back in the buffer
            console.warn("Skipping malformed stream line:", cleaned);
          }
        }
      } catch (err) {
        // catches "Error in input stream" or network disconnects
        controller.error(err);
      }
    },
    cancel() {
      reader.cancel();
    }
  });
};

export default fetchChatStream;