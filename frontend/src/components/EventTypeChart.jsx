import React, { useState } from 'react';
import { PieChart } from 'lucide-react';

const COLOR_MAP = {
  TRANSACTION_SETTLED: '#6366f1',
  POLICY_RENEWAL: '#10b981',
  KYC_VERIFICATION: '#f59e0b',
  SECURITY_ALERT: '#06b6d4',
  NOTIFICATION: '#a855f7'
};

const FALLBACK_COLORS = ['#ec4899', '#3b82f6', '#14b8a6', '#eab308', '#f97316'];

export default function EventTypeChart({ eventsByType = {} }) {
  const [hoveredSlice, setHoveredSlice] = useState(null);

  const entries = Object.entries(eventsByType || {});
  const total = entries.reduce((acc, [, val]) => acc + val, 0);

  // Fallback sample data if empty
  const displayEntries = entries.length > 0 ? entries : [
    ['TRANSACTION_SETTLED', 1],
    ['POLICY_RENEWAL', 1],
    ['KYC_VERIFICATION', 1],
    ['SECURITY_ALERT', 1]
  ];
  const isMock = entries.length === 0;
  const currentTotal = isMock ? 4 : total;

  // Compute SVG arc angles
  let accumulatedAngle = 0;
  const radius = 68;
  const strokeWidth = 24;
  const circumference = 2 * Math.PI * radius;

  const slices = displayEntries.map(([key, count], index) => {
    const percentage = currentTotal > 0 ? count / currentTotal : 0;
    const strokeDasharray = `${percentage * circumference} ${circumference}`;
    const strokeDashoffset = -accumulatedAngle * circumference;
    accumulatedAngle += percentage;
    const color = COLOR_MAP[key] || FALLBACK_COLORS[index % FALLBACK_COLORS.length];

    return {
      key,
      count,
      percentage: Math.round(percentage * 100),
      color,
      strokeDasharray,
      strokeDashoffset
    };
  });

  return (
    <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-xl flex flex-col justify-between">
      <div className="flex items-center justify-between mb-2">
        <h2 className="text-xs font-semibold text-slate-200 uppercase tracking-wider flex items-center gap-2">
          <PieChart className="w-4 h-4 text-indigo-400" /> Event Type Distribution
        </h2>
        {isMock && (
          <span className="text-[10px] text-slate-500 bg-slate-800/80 px-2 py-0.5 rounded">Default Preview</span>
        )}
      </div>

      <div className="relative flex items-center justify-center py-2">
        <svg className="w-40 h-40 transform -rotate-90" viewBox="0 0 180 180">
          <circle
            cx="90"
            cy="90"
            r={radius}
            fill="transparent"
            stroke="#1e293b"
            strokeWidth={strokeWidth}
          />
          {slices.map((slice, i) => (
            <circle
              key={i}
              cx="90"
              cy="90"
              r={radius}
              fill="transparent"
              stroke={slice.color}
              strokeWidth={hoveredSlice === slice.key ? strokeWidth + 4 : strokeWidth}
              strokeDasharray={slice.strokeDasharray}
              strokeDashoffset={slice.strokeDashoffset}
              strokeLinecap="round"
              className="transition-all duration-300 cursor-pointer"
              onMouseEnter={() => setHoveredSlice(slice.key)}
              onMouseLeave={() => setHoveredSlice(null)}
            />
          ))}
        </svg>

        <div className="absolute inset-0 flex flex-col items-center justify-center pointer-events-none">
          <span className="text-lg font-bold text-white font-mono">
            {hoveredSlice
              ? slices.find(s => s.key === hoveredSlice)?.count ?? currentTotal
              : currentTotal}
          </span>
          <span className="text-[10px] text-slate-400 font-medium">
            {hoveredSlice ? `${slices.find(s => s.key === hoveredSlice)?.percentage}%` : 'Total Events'}
          </span>
        </div>
      </div>

      <div className="grid grid-cols-2 gap-2 mt-3 text-[11px]">
        {slices.map((slice) => (
          <div
            key={slice.key}
            onMouseEnter={() => setHoveredSlice(slice.key)}
            onMouseLeave={() => setHoveredSlice(null)}
            className={`flex items-center justify-between p-1.5 rounded-lg border transition cursor-pointer ${
              hoveredSlice === slice.key
                ? 'bg-slate-800/80 border-slate-700'
                : 'bg-slate-950/60 border-slate-800/80 hover:border-slate-700'
            }`}
          >
            <div className="flex items-center gap-1.5 truncate pr-1">
              <span
                className="w-2.5 h-2.5 rounded-full flex-shrink-0"
                style={{ backgroundColor: slice.color }}
              />
              <span className="text-slate-300 truncate font-mono text-[10px]">{slice.key}</span>
            </div>
            <span className="text-slate-400 font-semibold font-mono text-[10px]">
              {slice.percentage}%
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}
