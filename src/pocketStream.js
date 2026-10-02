
export class PocketStream {
  constructor({ onStatus, onAssets, onQuote }) {
    this.onStatus = onStatus;
    this.onAssets = onAssets;
    this.onQuote = onQuote;
    this.socket = null;
    this.stopped = true;
  }

  connect(url) {
    this.disconnect();
    this.stopped = false;

    if (!url || !url.startsWith("wss://")) {
      this.onStatus("unverified");
      return;
    }

    this.onStatus("connecting");

    try {
      this.socket = new WebSocket(url);

      this.socket.onopen = () => {
        this.onStatus("connected");
      };

      this.socket.onmessage = (event) => {
        // The real Pocket Option protocol parser must be added
        // after verifying the actual handshake and event format.
        // Unknown messages are intentionally ignored.
        try {
          const message = JSON.parse(event.data);
          this.handleVerifiedMessage(message);
        } catch {
          // Socket.IO frames may not be plain JSON.
          // Do not interpret unknown frames as market data.
        }
      };

      this.socket.onerror = () => {
        this.onStatus("error");
      };

      this.socket.onclose = () => {
        this.onStatus(this.stopped ? "disconnected" : "closed");
      };
    } catch {
      this.onStatus("error");
    }
  }

  handleVerifiedMessage(message) {
    // Only map fields here once the live protocol has been
    // confirmed from real Pocket Option websocket messages.
    // Never add guessed symbols, prices, or event names.
    if (!message || typeof message !== "object") return;

    // Intentionally no inferred parsing in this stage.
  }

  disconnect() {
    this.stopped = true;

    if (this.socket) {
      this.socket.close();
      this.socket = null;
    }

    this.onStatus("disconnected");
  }
}
