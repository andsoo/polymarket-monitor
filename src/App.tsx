import { useEffect, useState } from "react";
import { Alert } from "./types";
import { Activity, AlertTriangle, Clock, ExternalLink, ShieldAlert, TrendingUp, Wallet, Play, Square, Database } from "lucide-react";
import { formatDistanceToNow } from "date-fns";
import { cn } from "./lib/utils";

export default function App() {
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [status, setStatus] = useState({ connected: false, message: "Connecting..." });
  const [stats, setStats] = useState({ totalEvents: 0, tradeEvents: 0, activeAssets: 0 });
  const [isMonitoring, setIsMonitoring] = useState(true);

  useEffect(() => {
    // Fetch initial status
    fetch('/api/status')
      .then(res => res.json())
      .then(data => setIsMonitoring(data.isMonitoring))
      .catch(console.error);

    const eventSource = new EventSource("/api/stream");

    eventSource.onmessage = (event) => {
      try {
        const newAlert = JSON.parse(event.data) as Alert;
        setAlerts((prev) => {
          const existingIndex = prev.findIndex(a => a.id === newAlert.id);
          if (existingIndex >= 0) {
            const updated = [...prev];
            updated[existingIndex] = { ...updated[existingIndex], ...newAlert };
            return updated;
          }
          return [newAlert, ...prev].slice(0, 100);
        });
      } catch (e) {
        console.error("Failed to parse alert", e);
      }
    };

    eventSource.addEventListener("status", (event) => {
      try {
        const newStatus = JSON.parse(event.data);
        setStatus(newStatus);
      } catch (e) {
        console.error("Failed to parse status", e);
      }
    });

    eventSource.addEventListener("stats", (event) => {
      try {
        setStats(JSON.parse(event.data));
      } catch (e) {
        console.error("Failed to parse stats", e);
      }
    });

    eventSource.onerror = () => {
      setStatus({ connected: false, message: "Disconnected from server" });
    };

    return () => {
      eventSource.close();
    };
  }, []);

  const handleStart = async () => {
    try {
      await fetch('/api/start', { method: 'POST' });
      setIsMonitoring(true);
    } catch (e) {
      console.error("Failed to start", e);
    }
  };

  const handleStop = async () => {
    try {
      await fetch('/api/stop', { method: 'POST' });
      setIsMonitoring(false);
    } catch (e) {
      console.error("Failed to stop", e);
    }
  };

  return (
    <div className="min-h-screen bg-gray-950 text-gray-100 font-sans selection:bg-blue-500/30">
      {/* Header */}
      <header className="sticky top-0 z-10 border-b border-gray-800 bg-gray-950/80 backdrop-blur-md">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 h-16 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="p-2 bg-blue-500/10 rounded-lg">
              <Activity className="w-5 h-5 text-blue-400" />
            </div>
            <h1 className="text-lg font-semibold tracking-tight text-gray-100">
              Polymarket Whale Monitor
            </h1>
          </div>
          <div className="flex items-center gap-3">
            {isMonitoring ? (
              <button
                onClick={handleStop}
                className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-red-500/10 hover:bg-red-500/20 text-red-400 text-sm transition-colors border border-red-500/20"
              >
                <Square className="w-4 h-4 fill-current" />
                <span className="hidden sm:inline">Stop</span>
              </button>
            ) : (
              <button
                onClick={handleStart}
                className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-emerald-500/10 hover:bg-emerald-500/20 text-emerald-400 text-sm transition-colors border border-emerald-500/20"
              >
                <Play className="w-4 h-4 fill-current" />
                <span className="hidden sm:inline">Start</span>
              </button>
            )}

            <div className="flex items-center gap-2 px-3 py-1.5 rounded-full bg-gray-900 border border-gray-800 text-sm">
              <div className={cn("w-2 h-2 rounded-full", status.connected ? "bg-emerald-500 animate-pulse" : "bg-red-500")} />
              <span className="text-gray-400">{status.message}</span>
            </div>
          </div>
        </div>
      </header>

      {/* Stats Bar */}
      <div className="bg-gray-900/50 border-b border-gray-800 py-2.5 backdrop-blur-md">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 flex flex-wrap items-center gap-x-8 gap-y-2 text-sm text-gray-400">
          <div className="flex items-center gap-2">
            <Activity className="w-4 h-4 text-blue-400" />
            <span><strong className="text-gray-200 font-mono">{stats.activeAssets}</strong> Assets Monitored</span>
          </div>
          <div className="flex items-center gap-2">
            <TrendingUp className="w-4 h-4 text-emerald-400" />
            <span><strong className="text-gray-200 font-mono">{stats.tradeEvents.toLocaleString()}</strong> Trades Processed</span>
          </div>
          <div className="flex items-center gap-2">
            <Database className="w-4 h-4 text-purple-400" />
            <span><strong className="text-gray-200 font-mono">{stats.totalEvents.toLocaleString()}</strong> Total Events</span>
          </div>
        </div>
      </div>

      {/* Main Content */}
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="flex items-center justify-between mb-6">
          <h2 className="text-xl font-medium text-gray-200">Live Alerts</h2>
          <div className="text-sm text-gray-500">
            Showing latest {alerts.length} events
          </div>
        </div>

        {alerts.length === 0 ? (
          <div className="flex flex-col items-center justify-center py-24 text-center border border-gray-800 border-dashed rounded-2xl bg-gray-900/30">
            <div className="w-16 h-16 mb-4 rounded-full bg-gray-800 flex items-center justify-center">
              <Activity className="w-8 h-8 text-gray-500" />
            </div>
            <h3 className="text-lg font-medium text-gray-300 mb-2">Listening for whales...</h3>
            <p className="text-gray-500 max-w-md">
              Monitoring the top 50 Polymarket order books for asymmetric trades &gt; $5,000 from new wallets.
            </p>
          </div>
        ) : (
          <div className="space-y-4">
            {alerts.map((alert) => (
              <AlertCard key={alert.id} alert={alert} />
            ))}
          </div>
        )}
      </main>
    </div>
  );
}

function AlertCard({ alert }: { alert: Alert }) {
  const isPending = alert.status === 'pending';
  const isSkipped = alert.status === 'skipped';
  const isPassed = alert.status === 'passed';

  const isHighConviction = isPassed && (alert.score ?? 0) >= 4;
  const isUrgent = (alert.hoursToResolution ?? 999) <= 24;
  const isHighAbsorption = isPassed && (alert.absorptionPct ?? 0) >= 0.025;

  return (
    <div className={cn(
      "relative overflow-hidden rounded-xl border p-5 transition-all",
      isHighConviction 
        ? "bg-red-950/20 border-red-900/50 hover:border-red-800/80" 
        : isSkipped
        ? "bg-gray-900/30 border-gray-800/50 opacity-60"
        : "bg-gray-900 border-gray-800 hover:border-gray-700"
    )}>
      {/* High Conviction Glow */}
      {isHighConviction && (
        <div className="absolute top-0 left-0 w-1 h-full bg-red-500 shadow-[0_0_10px_rgba(239,68,68,0.5)]" />
      )}

      <div className="flex flex-col lg:flex-row gap-6">
        {/* Left Column: Market & Trade Info */}
        <div className="flex-1 space-y-4">
          <div className="flex items-start justify-between gap-4">
            <div>
              <div className="flex items-center gap-2 mb-1.5">
                {isPending ? (
                  <span className="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-md text-xs font-medium bg-gray-800 text-gray-400 border border-gray-700">
                    <Activity className="w-3.5 h-3.5 animate-pulse" />
                    Checking Wallet Age...
                  </span>
                ) : isSkipped ? (
                  <span className="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-md text-xs font-medium bg-gray-800 text-gray-500 border border-gray-700">
                    <Square className="w-3.5 h-3.5" />
                    Skipped (Wallet &gt; 7 days)
                  </span>
                ) : isHighConviction ? (
                  <span className="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-md text-xs font-medium bg-red-500/10 text-red-400 border border-red-500/20">
                    <ShieldAlert className="w-3.5 h-3.5" />
                    High Conviction Insider
                  </span>
                ) : isHighAbsorption ? (
                  <span className="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-md text-xs font-medium bg-orange-500/10 text-orange-400 border border-orange-500/20">
                    <TrendingUp className="w-3.5 h-3.5" />
                    Urgent Liquidity Sweep
                  </span>
                ) : (
                  <span className="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-md text-xs font-medium bg-blue-500/10 text-blue-400 border border-blue-500/20">
                    <AlertTriangle className="w-3.5 h-3.5" />
                    New Wallet Anomaly
                  </span>
                )}
                {isPassed && (
                  <span className="text-xs text-gray-500 font-medium">
                    Score: {alert.score}/5
                  </span>
                )}
              </div>
              <h3 className={cn("text-lg font-medium leading-snug", isSkipped ? "text-gray-400" : "text-gray-100")}>
                {alert.marketTitle}
              </h3>
            </div>
            <div className="text-right shrink-0">
              <div className="text-xs text-gray-500 mb-1">
                {formatDistanceToNow(alert.timestamp, { addSuffix: true })}
              </div>
              <div className={cn(
                "inline-flex items-center justify-center px-3 py-1 rounded-lg font-mono text-sm font-medium",
                isSkipped ? "bg-gray-800 text-gray-500" :
                alert.side.toUpperCase() === "BUY" ? "bg-emerald-500/10 text-emerald-400" : "bg-rose-500/10 text-rose-400"
              )}>
                {alert.side.toUpperCase()} @ {(alert.price * 100).toFixed(1)}¢
              </div>
            </div>
          </div>

          <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 pt-2 border-t border-gray-800/50">
            <div>
              <div className="text-xs text-gray-500 mb-1">Size (USDC)</div>
              <div className={cn("font-mono text-lg font-medium", isSkipped ? "text-gray-500" : "text-gray-200")}>
                ${alert.usdAmount.toLocaleString(undefined, { maximumFractionDigits: 0 })}
              </div>
            </div>
            <div>
              <div className="text-xs text-gray-500 mb-1">Book Absorption</div>
              <div className={cn("font-mono text-lg font-medium", isSkipped ? "text-gray-500" : "text-gray-200")}>
                {alert.absorptionPct !== null ? `${(alert.absorptionPct * 100).toFixed(1)}%` : "..."}
              </div>
            </div>
            <div>
              <div className="text-xs text-gray-500 mb-1">Wallet Age</div>
              <div className={cn("font-mono text-lg font-medium", isSkipped ? "text-gray-500" : "text-gray-200")}>
                {alert.walletAgeDays === null ? "..." : alert.walletAgeDays === 999 ? "Unknown" : `${alert.walletAgeDays.toFixed(1)}d`}
              </div>
            </div>
            <div>
              <div className="text-xs text-gray-500 mb-1">Resolves In</div>
              <div className={cn("font-mono text-lg font-medium flex items-center gap-1.5", isSkipped ? "text-gray-500" : "text-gray-200")}>
                {alert.hoursToResolution === null ? "..." : alert.hoursToResolution > 9999 ? "Unknown" : `${Math.max(0, alert.hoursToResolution).toFixed(1)}h`}
                {isUrgent && !isSkipped && <Clock className="w-4 h-4 text-orange-400" />}
              </div>
            </div>
          </div>
        </div>

        {/* Right Column: Links & Wallet */}
        <div className="lg:w-64 shrink-0 flex flex-col justify-center gap-3 p-4 bg-gray-950/50 rounded-lg border border-gray-800/50">
          <div className="flex items-center gap-2 text-sm">
            <Wallet className="w-4 h-4 text-gray-500" />
            <span className="text-gray-400">Wallet</span>
          </div>
          {isPending ? (
            <div className="font-mono text-sm text-gray-500">Checking...</div>
          ) : (
            <a 
              href={`https://polygonscan.com/address/${alert.wallet}`}
              target="_blank"
              rel="noopener noreferrer"
              className="font-mono text-sm text-blue-400 hover:text-blue-300 truncate block transition-colors"
              title={alert.wallet}
            >
              {alert.wallet !== "Unknown" && alert.wallet !== "Error" ? `${alert.wallet.slice(0, 8)}...${alert.wallet.slice(-6)}` : alert.wallet}
            </a>
          )}
          
          <div className="h-px bg-gray-800 my-1" />
          
          <a
            href={`https://polygonscan.com/tx/${alert.txHash}`}
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center justify-between text-sm text-gray-400 hover:text-gray-200 transition-colors group"
          >
            <span>View Transaction</span>
            <ExternalLink className="w-4 h-4 opacity-50 group-hover:opacity-100 transition-opacity" />
          </a>
        </div>
      </div>
    </div>
  );
}
