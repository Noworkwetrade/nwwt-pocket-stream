
import { useMemo, useState } from "react";

const statusText = {
  unverified: "FEED NOT VERIFIED",
  connecting: "CONNECTING",
  connected: "SOCKET CONNECTED · DATA UNVERIFIED",
  error: "CONNECTION ERROR",
  closed: "CONNECTION CLOSED",
  disconnected: "DISCONNECTED"
};

export default function App() {
  const [filter, setFilter] = useState("all");
  const [search, setSearch] = useState("");
  const [status] = useState("unverified");
  const [assets] = useState([]);

  const visibleAssets = useMemo(() => {
    return assets.filter((asset) => {
      const matchesFilter =
        filter === "all" || asset.market === filter;
      const matchesSearch = asset.symbol
        .toLowerCase()
        .includes(search.toLowerCase());

      return matchesFilter && matchesSearch;
    });
  }, [assets, filter, search]);

  return (
    <main className="shell">
      <header className="topbar">
        <div className="brand">
          <div className="brand-mark">N</div>
          <div>
            <strong>NWWT</strong>
            <small>POCKET STREAM</small>
          </div>
        </div>

        <div className="connection">
          <span className={`status-dot ${status}`} />
          {statusText[status]}
        </div>
      </header>

      <section className="intro">
        <p className="eyebrow">LIVE MARKET WATCHLIST</p>
        <h1>
          Market data,
          <span> without the noise</span>
        </h1>
        <p className="subtitle">
          Pocket Option OTC and regular forex market stream
        </p>
      </section>

      <section className="toolbar">
        <div className="filters">
          {["all", "otc", "forex"].map((item) => (
            <button
              key={item}
              className={filter === item ? "active" : ""}
              onClick={() => setFilter(item)}
            >
              {item === "all"
                ? "All assets"
                : item.toUpperCase()}
            </button>
          ))}
        </div>

        <input
          value={search}
          onChange={(event) => setSearch(event.target.value)}
          placeholder="Search assets"
          aria-label="Search assets"
        />
      </section>

      <section className="watchlist">
        <div className="table-head">
          <span>ASSET</span>
          <span>MARKET</span>
          <span className="right">LIVE PRICE</span>
          <span className="right">CHANGE</span>
        </div>

        {visibleAssets.length > 0 ? (
          visibleAssets.map((asset) => (
            <div className="asset-row" key={asset.symbol}>
              <strong>{asset.symbol}</strong>
              <span>{asset.market.toUpperCase()}</span>
              <span className="right">{asset.price}</span>
              <span className={`right ${asset.change >= 0 ? "up" : "down"}`}>
                {asset.change}%
              </span>
            </div>
          ))
        ) : (
          <div className="empty">
            <div className="empty-icon">◉</div>
            <strong>Waiting for verified market data</strong>
            <p>
              No sample prices or assets are shown
              Market rows will appear only after a real
              Pocket Option feed is connected and parsed
            </p>
          </div>
        )}
      </section>

      <footer>
        <span>NO MOCK DATA · NO TRADE EXECUTION</span>
        <span>LIVE FEED NOT YET VERIFIED</span>
      </footer>
    </main>
  );
}
