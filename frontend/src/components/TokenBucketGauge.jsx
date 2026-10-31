import React from 'react';
import { Coins, ShieldCheck, Zap } from 'lucide-react';

export default function TokenBucketGauge({ available = 15, capacity = 15 }) {
  const safeCapacity = capacity > 0 ? capacity : 15;
  const safeAvailable = Math.min(safeCapacity, Math.max(0, available ?? safeCapacity));
  const pct = Math.round((safeAvailable / safeCapacity) * 100);

  return (
    <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-xl flex flex-col justify-between">
      <div className="flex items-center justify-between mb-3">
        <h2 className="text-xs font-semibold text-slate-200 uppercase tracking-wider flex items-center gap-2">
          <Coins className="w-4 h-4 text-amber-400" /> Token Bucket Gauge
        </h2>
        <span className="text-[10px] text-amber-400/90 font-mono flex items-center gap-1 bg-amber-500/10 px-2 py-0.5 rounded border border-amber-500/20">
          <Zap className="w-3 h-3" /> Rate Limiter
        </span>
      </div>

      <div className="bg-slate-950 rounded-xl p-4 border border-slate-800 text-center">
        <div className="text-xs text-slate-400 font-medium">Available Burst Capacity</div>

        <div className="my-3">
          <div className="w-full bg-slate-800/90 h-4 rounded-full overflow-hidden p-0.5 shadow-inner">
            <div
              className={`h-full rounded-full transition-all duration-500 ${
                pct < 20
                  ? 'bg-rose-500 shadow-rose-500/50'
                  : pct < 50
                  ? 'bg-amber-500 shadow-amber-500/50'
                  : 'bg-gradient-to-r from-amber-500 via-sky-400 to-emerald-400 shadow-emerald-500/40'
              }`}
              style={{ width: `${pct}%` }}
            />
          </div>
        </div>

        <div className="flex justify-between text-xs font-mono text-slate-400 px-1">
          <span>0 Tokens</span>
          <span className={`font-bold ${pct < 20 ? 'text-rose-400' : 'text-emerald-400'}`}>
            {safeAvailable} / {safeCapacity} Available ({pct}%)
          </span>
          <span>{safeCapacity} Max</span>
        </div>

        <div className="mt-3 text-[11px] text-slate-400 bg-slate-900/80 py-2 px-3 rounded-lg border border-slate-800/90 flex items-center justify-center gap-1.5">
          <ShieldCheck className="w-3.5 h-3.5 text-emerald-400" />
          <span>Refill Rate: <strong className="text-slate-200">+5 tokens / sec</strong> • Burst Cap: <strong className="text-slate-200">{safeCapacity} reqs</strong></span>
        </div>
      </div>
    </div>
  );
}
