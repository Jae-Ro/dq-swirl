import { Streamdown } from 'streamdown';

function ChatMessage({ streamText, isStreaming }) {
  return (
    <div className="p-4 border rounded">
      <Streamdown isAnimating={isStreaming}>
        {streamText}
      </Streamdown>
    </div>
  );
}