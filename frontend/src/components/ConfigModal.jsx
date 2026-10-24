import React, { useState } from 'react';
import { Settings, X, CheckCircle2, AlertTriangle, RefreshCw, Database, Layers, Flame } from 'lucide-react';
import { getApiBaseUrl, setApiBaseUrl, testCloudConnections } from '../services/api';

export default function ConfigModal({ isOpen, onClose, onConfigSaved }) {
  const [url, setUrl] = useState(getApiBaseUrl() || '');
  const [testing, setTesting] = useState(false);
  const [report, setReport] = useState(null);
  const [errorMessage, setErrorMessage] = useState('');

  if (!isOpen) return null;

  const handleTest = async () => {
    setTesting(true);
    setReport(null);
    setErrorMessage('');
    try {
      const prev = getApiBaseUrl();
      setApiBaseUrl(url);
      const data = await testCloudConnections();
      setReport(data);
      setApiBaseUrl(prev);
    } catch (err) {
      setErrorMessage(`Diagnostics failed: ${err.message}`);
    } finally {
      setTesting(false);
    }
  };

  const handleSave = () => {
    setApiBaseUrl(url);
    if (onConfigSaved) onConfigSaved();
    onClose();
  };

  return (
    <div className="fixed inset-0 bg-slate-950/80 backdrop-blur-sm z-50 flex items-center justify-center p-4">
      <div className="bg-slate-900 border border-slate-800 rounded-2xl max-w-md w-full p-6 shadow-2xl space-y-4">
        <div className="flex justify-between items-center border-b border-slate-800 pb-3">
          <h3 className="text-sm font-bold text-white flex items-center gap-2">
            <Settings className="w-4 h-4 text-indigo-400" /> Backend & Cloud Connectivity
          </h3>
          <button onClick={onClose} className="text-slate-400 hover:text-white transition">
            <X className="w-4 h-4" />
          </button>
        </div>

        <div className="text-xs text-slate-400 leading-relaxed">
          Specify the target Spring Boot endpoint. Leave blank to use the local proxy (<code className="text-indigo-300">/api</code>).
        </div>

        <div>
          <label className="block text-xs text-slate-300 font-medium mb-1">
            Backend API Base URL
          </label>
          <input
            type="text"
            value={url}
            onChange={(e) => setUrl(e.target.value)}
            placeholder="http://localhost:8080 (or blank for same-origin)"
            className="w-full bg-slate-950 border border-slate-700 rounded-lg p-2.5 text-white font-mono text-xs focus:outline-none focus:border-indigo-500"
          />
        </div>

        {errorMessage && (
          <div className="p-3 rounded-lg text-xs flex items-center gap-2 border bg-rose-500/10 border-rose-500/20 text-rose-400">
            <AlertTriangle className="w-4 h-4 flex-shrink-0" />
            <span className="truncate">{errorMessage}</span>
          </div>
        )}

        {report && (
          <div className="space-y-2 text-xs border border-slate-800 rounded-xl p-3 bg-slate-950">
            <div className="font-semibold text-slate-300 text-[11px] uppercase tracking-wider mb-2">
              Cloud Infrastructure Health
            </div>

            {/* PostgreSQL */}
            <div className="flex justify-between items-center py-1 border-b border-slate-800/60">
              <span className="flex items-center gap-1.5 text-slate-300">
                <Database className="w-3.5 h-3.5 text-blue-400" /> PostgreSQL:
              </span>
              <span className={`px-2 py-0.5 rounded text-[10px] font-bold border ${
                report.postgresql?.status === 'CONNECTED'
                  ? 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20'
                  : 'bg-rose-500/10 text-rose-400 border-rose-500/20'
              }`}>
                {report.postgresql?.status} {report.postgresql?.latencyMs ? `(${report.postgresql.latencyMs}ms)` : ''}
              </span>
            </div>

            {/* Redis */}
            <div className="flex justify-between items-center py-1 border-b border-slate-800/60">
              <span className="flex items-center gap-1.5 text-slate-300">
                <Flame className="w-3.5 h-3.5 text-amber-400" /> Redis:
              </span>
              <span className={`px-2 py-0.5 rounded text-[10px] font-bold border ${
                report.redis?.status === 'CONNECTED'
                  ? 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20'
                  : 'bg-amber-500/10 text-amber-400 border-amber-500/20'
              }`}>
                {report.redis?.status}
              </span>
            </div>

            {/* Kafka */}
            <div className="flex justify-between items-center py-1">
              <span className="flex items-center gap-1.5 text-slate-300">
                <Layers className="w-3.5 h-3.5 text-purple-400" /> Kafka:
              </span>
              <span className={`px-2 py-0.5 rounded text-[10px] font-bold border ${
                report.kafka?.status === 'READY'
                  ? 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20'
                  : 'bg-indigo-500/10 text-indigo-400 border-indigo-500/20'
              }`}>
                {report.kafka?.status}
              </span>
            </div>
          </div>
        )}

        <div className="flex justify-between items-center pt-2 border-t border-slate-800">
          <button
            type="button"
            onClick={handleTest}
            disabled={testing}
            className="px-3 py-1.5 bg-slate-800 hover:bg-slate-700 text-slate-300 rounded-lg text-xs font-medium flex items-center gap-1.5 transition disabled:opacity-50"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${testing ? 'animate-spin' : ''}`} />
            {testing ? 'Testing...' : 'Test Connections'}
          </button>

          <div className="flex gap-2">
            <button
              type="button"
              onClick={onClose}
              className="px-3 py-1.5 bg-slate-800 hover:bg-slate-700 text-slate-300 rounded-lg text-xs transition"
            >
              Cancel
            </button>
            <button
              type="button"
              onClick={handleSave}
              className="px-4 py-1.5 bg-indigo-600 hover:bg-indigo-500 text-white rounded-lg text-xs font-bold shadow-md shadow-indigo-600/30 transition"
            >
              Save & Apply
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
