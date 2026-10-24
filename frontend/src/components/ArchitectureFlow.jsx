import React from 'react';
import { Network, UploadCloud, Filter, Fingerprint, Layers, Cpu, Database } from 'lucide-react';

const NODES = [
  {
    step: '1. Ingest API',
    desc: 'REST Endpoint /api/events/publish',
    icon: UploadCloud,
    color: 'indigo',
    borderHover: 'hover:border-indigo-500',
    iconBg: 'bg-indigo-500/10 text-indigo-400'
  },
  {
    step: '2. Rate Limiter',
    desc: 'Token Bucket (Cap: 25, Refill: 5/s)',
    icon: Filter,
    color: 'amber',
    borderHover: 'hover:border-amber-500',
    iconBg: 'bg-amber-500/10 text-amber-400'
  },
  {
    step: '3. Deduplication',
    desc: 'Redis SHA-256 (TTL: 10m)',
    icon: Fingerprint,
    color: 'cyan',
    borderHover: 'hover:border-cyan-500',
    iconBg: 'bg-cyan-500/10 text-cyan-400'
  },
  {
    step: '4. Apache Kafka',
    desc: '3 Partitions • Idempotent Producer',
    icon: Layers,
    color: 'purple',
    borderHover: 'hover:border-purple-500',
    iconBg: 'bg-purple-500/10 text-purple-400'
  },
  {
    step: '5. Consumer Engine',
    desc: 'Manual ACK + Retry / DLQ',
    icon: Cpu,
    color: 'emerald',
    borderHover: 'hover:border-emerald-500',
    iconBg: 'bg-emerald-500/10 text-emerald-400'
  },
  {
    step: '6. Storage & Alert',
    desc: 'PostgreSQL Audit + Webhooks',
    icon: Database,
    color: 'blue',
    borderHover: 'hover:border-blue-500',
    iconBg: 'bg-blue-500/10 text-blue-400'
  }
];

export default function ArchitectureFlow() {
  return (
    <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-xl">
      <div className="flex flex-wrap justify-between items-center mb-4 gap-2">
        <h2 className="text-xs font-semibold text-slate-200 uppercase tracking-wider flex items-center gap-2">
          <Network className="w-4 h-4 text-indigo-400" /> Event Processing Workflow Architecture
        </h2>
        <span className="text-xs text-slate-500 font-mono">
          Guaranteed At-Least-Once Delivery Semantics
        </span>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3 relative">
        {NODES.map((node, i) => {
          const Icon = node.icon;
          return (
            <div
              key={i}
              className={`bg-slate-950 border border-slate-800 rounded-xl p-3.5 text-center transition ${node.borderHover} shadow-sm relative group`}
            >
              <div
                className={`w-9 h-9 mx-auto mb-2 rounded-lg ${node.iconBg} flex items-center justify-center transition group-hover:scale-110 duration-200`}
              >
                <Icon className="w-4 h-4" />
              </div>
              <div className="text-xs font-bold text-slate-200">{node.step}</div>
              <div className="text-[11px] text-slate-400 mt-1 leading-tight">{node.desc}</div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
