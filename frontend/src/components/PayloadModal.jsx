import React, { useState } from 'react';
import { Code, X, Copy, Check } from 'lucide-react';

export default function PayloadModal({ payload, onClose }) {
  const [copied, setCopied] = useState(false);

  if (!payload) return null;

  let formatted = '';
  try {
    const obj = typeof payload === 'string' ? JSON.parse(payload) : payload;
    formatted = JSON.stringify(obj, null, 2);
  } catch (e) {
    formatted = String(payload);
  }

  const handleCopy = () => {
    navigator.clipboard.writeText(formatted);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="fixed inset-0 bg-slate-950/80 backdrop-blur-sm z-50 flex items-center justify-center p-4">
      <div className="bg-slate-900 border border-slate-800 rounded-2xl max-w-lg w-full p-6 shadow-2xl space-y-4">
        <div className="flex justify-between items-center border-b border-slate-800 pb-3">
          <h3 className="text-sm font-bold text-white flex items-center gap-2">
            <Code className="w-4 h-4 text-indigo-400" /> Event Payload Details
          </h3>
          <button onClick={onClose} className="text-slate-400 hover:text-white transition">
            <X className="w-4 h-4" />
          </button>
        </div>

        <pre className="bg-slate-950 p-4 rounded-xl text-xs font-mono text-emerald-400 overflow-x-auto max-h-80 border border-slate-800 custom-scrollbar leading-relaxed">
          {formatted}
        </pre>

        <div className="flex justify-between items-center pt-2">
          <button
            onClick={handleCopy}
            className="px-3 py-1.5 bg-slate-800 hover:bg-slate-700 text-slate-300 rounded-lg text-xs font-medium flex items-center gap-1.5 transition"
          >
            {copied ? (
              <>
                <Check className="w-3.5 h-3.5 text-emerald-400" />
                <span className="text-emerald-400">Copied!</span>
              </>
            ) : (
              <>
                <Copy className="w-3.5 h-3.5" />
                <span>Copy JSON</span>
              </>
            )}
          </button>

          <button
            onClick={onClose}
            className="px-4 py-1.5 bg-indigo-600 hover:bg-indigo-500 text-white rounded-lg text-xs font-medium transition"
          >
            Close
          </button>
        </div>
      </div>
    </div>
  );
}
