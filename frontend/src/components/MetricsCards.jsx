import React from 'react';
import { TrendingUp, CheckCircle, ShieldAlert, Copy, Gauge, Timer } from 'lucide-react';

export default function MetricsCards({ stats }) {
  const cards = [
    {
      title: 'Total Ingested',
      value: (stats.totalIngested ?? 0).toLocaleString(),
      subtext: 'Ingress API Stream',
      subIcon: TrendingUp,
      textColor: 'text-white',
      iconColor: 'text-sky-400'
    },
    {
      title: 'Successfully Processed',
      value: (stats.totalProcessed ?? 0).toLocaleString(),
      subtext: 'Committed to DB',
      subIcon: CheckCircle,
      textColor: 'text-emerald-400',
      iconColor: 'text-emerald-400'
    },
    {
      title: 'Rate-Limited (429)',
      value: (stats.totalRateLimited ?? 0).toLocaleString(),
      subtext: 'Token Bucket Throttled',
      subIcon: ShieldAlert,
      textColor: 'text-amber-400',
      iconColor: 'text-amber-400'
    },
    {
      title: 'Duplicates Suppressed',
      value: (stats.totalDuplicatesBlocked ?? 0).toLocaleString(),
      subtext: 'Redis Idempotency Hit',
      subIcon: Copy,
      textColor: 'text-cyan-400',
      iconColor: 'text-cyan-400'
    },
    {
      title: 'Throughput (EPS)',
      value: (stats.currentThroughputEps ?? 0).toFixed(1),
      subtext: 'Events per second',
      subIcon: Gauge,
      textColor: 'text-purple-400',
      iconColor: 'text-purple-400'
    },
    {
      title: 'Avg Latency',
      value: `${(stats.averageLatencyMs ?? 0).toFixed(1)} ms`,
      subtext: 'End-to-end processing',
      subIcon: Timer,
      textColor: 'text-indigo-400',
      iconColor: 'text-indigo-400'
    }
  ];

  return (
    <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
      {cards.map((c, i) => {
        const SubIcon = c.subIcon;
        return (
          <div
            key={i}
            className="bg-slate-900 border border-slate-800 rounded-xl p-4 shadow-md transition hover:border-slate-700"
          >
            <div className="text-xs text-slate-400 font-medium truncate">{c.title}</div>
            <div className={`text-2xl font-bold mt-1 font-mono ${c.textColor}`}>{c.value}</div>
            <div className="text-[10px] text-slate-500 mt-1 flex items-center gap-1.5 truncate">
              <SubIcon className={`w-3 h-3 ${c.iconColor}`} />
              <span>{c.subtext}</span>
            </div>
          </div>
        );
      })}
    </div>
  );
}
