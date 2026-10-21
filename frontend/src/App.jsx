import React, { useState, useEffect } from 'react';
import { 
  Activity, Zap, ShieldAlert, Copy, RefreshCw, Send, 
  Flame, Database, Filter, Layers, Server, Search, CheckCircle2, AlertTriangle
} from 'lucide-react';

export default function App() {
  const [stats, setStats] = useState({
    totalIngested: 0,
    totalProcessed: 0,
    totalRateLimited: 0,
    totalDuplicatesBlocked: 0,
    currentThroughputEps: 0.0,
    averageLatencyMs: 0.0,
    tokenBucketCapacity: 25,
    tokenBucketAvailable: 25,
    eventsByType: {}
  });

  const [liveEvents, setLiveEvents] = useState([]);
  const [auditLogs, setAuditLogs] = useState([]);
  const [loading, setLoading] = useState(false);
  const [bursting, setBursting] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [statusFilter, setStatusFilter] = useState('');
  const [selectedPayload, setSelectedPayload] = useState(null);
  const [isModalOpen, setIsModalOpen] = useState(false);

  // Form State
  const [formType, setFormType] = useState('TRANSACTION_SETTLED');
  const [formService, setFormService] = useState('payment-gateway-v2');
  const [formIdempotency, setFormIdempotency] = useState('');
  const [formPayload, setFormPayload] = useState(JSON.stringify({
    userId: "usr_99812",
    amount: 1450.50,
    currency: "INR",
    status: "AUTHORIZED"
  }, null, 2));

  // Poll stats and audit logs
  useEffect(() => {
    fetchStats();
    fetchAuditLogs();
    const interval = setInterval(fetchStats, 2000);
    return () => clearInterval(interval);
  }, []);

  // SSE Stream
  useEffect(() => {
    let eventSource;
    try {
      eventSource = new EventSource('/api/events/stream');
      eventSource.addEventListener('event', (e) => {
        try {
          const newEvent = JSON.parse(e.data);
          setLiveEvents(prev => [newEvent, ...prev.slice(0, 24)]);
        } catch (err) {
          console.error("SSE parse error", err);
        }
      });
    } catch (err) {
      console.warn("SSE stream unavailable", err);
    }
    return () => {
      if (eventSource) eventSource.close();
    };
  }, []);

  const fetchStats = async () => {
    try {
      const res = await fetch('/api/events/stats');
      if (res.ok) {
        const data = await res.json();
        setStats(data);
      }
    } catch (e) {
      // offline/local mock
    }
  };

  const fetchAuditLogs = async () => {
    setLoading(true);
    try {
      let url = '/api/events/audit?page=0&size=10';
      if (searchQuery) url += `&query=${encodeURIComponent(searchQuery)}`;
      if (statusFilter) url += `&status=${encodeURIComponent(statusFilter)}`;
      const res = await fetch(url);
      if (res.ok) {
        const data = await res.json();
        setAuditLogs(data.content || []);
      }
    } catch (e) {
      console.error(e);
    } finally {
      setLoading(false);
    }
  };

  const handlePublish = async (e) => {
    e.preventDefault();
    try {
      const payloadObj = JSON.parse(formPayload);
      const res = await fetch('/api/events/publish', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          eventType: formType,
          sourceService: formService,
          idempotencyKey: formIdempotency || null,
          data: payloadObj
        })
      });
      setIsModalOpen(false);
      fetchStats();
      fetchAuditLogs();
    } catch (err) {
      alert("Invalid JSON or server error: " + err.message);
    }
  };

  const handleBurst = async () => {
    setBursting(true);
    try {
      await fetch('/api/events/burst?count=35&includeDuplicates=true', { method: 'POST' });
      fetchStats();
      fetchAuditLogs();
    } catch (e) {
      console.error(e);
    } finally {
      setBursting(false);
    }
  };

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100 flex flex-col font-sans">
      
      {/* Top Navigation */}
      <header className="border-b border-slate-800 bg-slate-900/90 backdrop-blur sticky top-0 z-40 px-6 py-4">
        <div className="max-w-7xl mx-auto flex flex-wrap justify-between items-center gap-4">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-xl bg-gradient-to-tr from-indigo-600 to-emerald-400 flex items-center justify-center text-white shadow-lg shadow-indigo-500/20">
              <Zap className="w-5 h-5" />
            </div>
            <div>
              <div className="flex items-center gap-2">
                <h1 className="font-bold text-base text-white tracking-tight">Event-Driven Notification & Audit Pipeline</h1>
                <span className="text-[10px] font-bold px-2 py-0.5 rounded-full bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">READY</span>
              </div>
              <p className="text-xs text-slate-400">Java 21 • Spring Boot 3.3 • Apache Kafka • Redis Token Bucket • PostgreSQL</p>
            </div>
          </div>

          <div className="flex items-center gap-3">
            <button 
              onClick={() => setIsModalOpen(true)}
              className="px-3.5 py-2 bg-indigo-600 hover:bg-indigo-500 text-white font-medium text-xs rounded-lg transition shadow-md shadow-indigo-600/30 flex items-center gap-2">
              <Send className="w-3.5 h-3.5" /> Publish Event
            </button>
            <button 
              onClick={handleBurst} 
              disabled={bursting}
              className="px-3.5 py-2 bg-amber-600 hover:bg-amber-500 text-white font-medium text-xs rounded-lg transition shadow-md shadow-amber-600/30 flex items-center gap-2 disabled:opacity-50">
              <Flame className="w-3.5 h-3.5" /> {bursting ? 'Simulating Burst...' : 'Simulate Traffic Burst (35 Reqs)'}
            </button>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="max-w-7xl mx-auto px-6 py-6 flex-1 w-full space-y-6">

        {/* Architecture Flow Banner */}
        <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-xl">
          <div className="flex justify-between items-center mb-4">
            <h2 className="text-xs font-bold text-slate-300 uppercase tracking-wider flex items-center gap-2">
              <Layers className="w-4 h-4 text-indigo-400" /> Pipeline Architecture & Delivery Guarantees
            </h2>
            <span className="text-[11px] text-slate-500 font-mono">Manual ACK • Idempotent Consumer • Dead Letter Queue</span>
          </div>

          <div className="grid grid-cols-2 md:grid-cols-6 gap-3">
            <div className="bg-slate-950 border border-slate-800 rounded-xl p-3 text-center">
              <Server className="w-5 h-5 text-indigo-400 mx-auto mb-1.5" />
              <div className="text-xs font-bold text-white">1. Ingest API</div>
              <div className="text-[10px] text-slate-400 mt-0.5">Spring Boot REST</div>
            </div>
            <div className="bg-slate-950 border border-slate-800 rounded-xl p-3 text-center">
              <Filter className="w-5 h-5 text-amber-400 mx-auto mb-1.5" />
              <div className="text-xs font-bold text-white">2. Rate Limiter</div>
              <div className="text-[10px] text-slate-400 mt-0.5">Token Bucket (25/5s)</div>
            </div>
            <div className="bg-slate-950 border border-slate-800 rounded-xl p-3 text-center">
              <Copy className="w-5 h-5 text-cyan-400 mx-auto mb-1.5" />
              <div className="text-xs font-bold text-white">3. Deduplication</div>
              <div className="text-[10px] text-slate-400 mt-0.5">Redis SHA-256 TTL</div>
            </div>
            <div className="bg-slate-950 border border-slate-800 rounded-xl p-3 text-center">
              <Zap className="w-5 h-5 text-purple-400 mx-auto mb-1.5" />
              <div className="text-xs font-bold text-white">4. Apache Kafka</div>
              <div className="text-[10px] text-slate-400 mt-0.5">3 Partitions (Acks=All)</div>
            </div>
            <div className="bg-slate-950 border border-slate-800 rounded-xl p-3 text-center">
              <Activity className="w-5 h-5 text-emerald-400 mx-auto mb-1.5" />
              <div className="text-xs font-bold text-white">5. Consumer</div>
              <div className="text-[10px] text-slate-400 mt-0.5">At-Least-Once ACK</div>
            </div>
            <div className="bg-slate-950 border border-slate-800 rounded-xl p-3 text-center">
              <Database className="w-5 h-5 text-blue-400 mx-auto mb-1.5" />
              <div className="text-xs font-bold text-white">6. Audit & Alert</div>
              <div className="text-[10px] text-slate-400 mt-0.5">PostgreSQL + Dispatch</div>
            </div>
          </div>
        </div>

        {/* Metrics Grid */}
        <div className="grid grid-cols-2 md:grid-cols-6 gap-4">
          <div className="bg-slate-900 border border-slate-800 rounded-xl p-4">
            <div className="text-xs text-slate-400">Total Ingested</div>
            <div className="text-2xl font-bold text-white mt-1">{stats.totalIngested}</div>
          </div>
          <div className="bg-slate-900 border border-slate-800 rounded-xl p-4">
            <div className="text-xs text-slate-400">Committed Events</div>
            <div className="text-2xl font-bold text-emerald-400 mt-1">{stats.totalProcessed}</div>
          </div>
          <div className="bg-slate-900 border border-slate-800 rounded-xl p-4">
            <div className="text-xs text-slate-400">Rate-Limited (429)</div>
            <div className="text-2xl font-bold text-amber-400 mt-1">{stats.totalRateLimited}</div>
          </div>
          <div className="bg-slate-900 border border-slate-800 rounded-xl p-4">
            <div className="text-xs text-slate-400">Duplicates Suppressed</div>
            <div className="text-2xl font-bold text-cyan-400 mt-1">{stats.totalDuplicatesBlocked}</div>
          </div>
          <div className="bg-slate-900 border border-slate-800 rounded-xl p-4">
            <div className="text-xs text-slate-400">Throughput</div>
            <div className="text-2xl font-bold text-purple-400 mt-1">{stats.currentThroughputEps.toFixed(1)} <span className="text-xs text-slate-500 font-normal">EPS</span></div>
          </div>
          <div className="bg-slate-900 border border-slate-800 rounded-xl p-4">
            <div className="text-xs text-slate-400">Avg Latency</div>
            <div className="text-2xl font-bold text-indigo-400 mt-1">{stats.averageLatencyMs} <span className="text-xs text-slate-500 font-normal">ms</span></div>
          </div>
        </div>

        {/* Live Stream & Audit Table */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">

          {/* Left: Live Stream */}
          <div className="lg:col-span-1 bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-xl flex flex-col h-[520px]">
            <div className="flex justify-between items-center mb-3">
              <h2 className="text-xs font-bold text-slate-200 uppercase tracking-wider flex items-center gap-2">
                <span className="w-2 h-2 rounded-full bg-emerald-400 animate-pulse"></span>
                Live Event Ingress Stream
              </h2>
              <button onClick={() => setLiveEvents([])} className="text-[11px] text-slate-500 hover:text-slate-300">Clear</button>
            </div>

            <div className="flex-1 overflow-y-auto space-y-2 pr-1 text-xs">
              {liveEvents.length === 0 ? (
                <div className="text-center py-16 text-slate-500 text-xs">
                  No live events yet. Click <strong>"Publish Event"</strong> or <strong>"Traffic Burst"</strong> to test!
                </div>
              ) : (
                liveEvents.map((evt, idx) => (
                  <div key={idx} className="bg-slate-950 border border-slate-800/80 rounded-xl p-3 flex justify-between items-center">
                    <div>
                      <div className="font-bold text-white font-mono text-[11px]">{evt.eventId?.substring(0, 16)}</div>
                      <div className="text-[10px] text-slate-400 mt-0.5">
                        <span className="text-indigo-400 font-semibold">{evt.eventType}</span> • Part: {evt.partitionNumber ?? '-'}
                      </div>
                    </div>
                    <div className="text-right">
                      <span className={`px-2 py-0.5 rounded text-[9px] font-bold border ${
                        evt.status === 'PROCESSED' ? 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20' :
                        evt.status === 'RATE_LIMITED' ? 'bg-amber-500/10 text-amber-400 border-amber-500/20' :
                        'bg-cyan-500/10 text-cyan-400 border-cyan-500/20'
                      }`}>
                        {evt.status}
                      </span>
                      <div className="text-[10px] text-slate-500 font-mono mt-1">{evt.processingLatencyMs || 1}ms</div>
                    </div>
                  </div>
                ))
              )}
            </div>
          </div>

          {/* Right: PostgreSQL Audit Logs */}
          <div className="lg:col-span-2 bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-xl flex flex-col h-[520px]">
            <div className="flex flex-wrap justify-between items-center gap-3 mb-4">
              <h2 className="text-xs font-bold text-slate-200 uppercase tracking-wider flex items-center gap-2">
                <Database className="w-4 h-4 text-blue-400" /> PostgreSQL Audit Trail Logs
              </h2>
              <div className="flex gap-2">
                <input 
                  type="text" 
                  value={searchQuery}
                  onChange={e => setSearchQuery(e.target.value)}
                  placeholder="Filter by event, service..." 
                  className="bg-slate-950 border border-slate-700 rounded-lg px-3 py-1 text-xs text-white placeholder-slate-500 focus:outline-none focus:border-indigo-500 w-48"
                />
                <button onClick={fetchAuditLogs} className="bg-slate-800 hover:bg-slate-700 text-slate-200 px-3 py-1 rounded-lg text-xs">
                  <Search className="w-3.5 h-3.5" />
                </button>
              </div>
            </div>

            <div className="flex-1 overflow-x-auto">
              <table className="w-full text-left text-xs text-slate-300">
                <thead className="bg-slate-950/70 text-slate-400 uppercase font-mono text-[10px] border-b border-slate-800">
                  <tr>
                    <th className="py-2 px-3">Event ID</th>
                    <th className="py-2 px-3">Type</th>
                    <th className="py-2 px-3">Status</th>
                    <th className="py-2 px-3">Part</th>
                    <th className="py-2 px-3">Latency</th>
                    <th className="py-2 px-3">Time</th>
                    <th className="py-2 px-3 text-right">Action</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-800/60 font-mono text-[11px]">
                  {auditLogs.length === 0 ? (
                    <tr><td colSpan="7" className="text-center py-10 text-slate-500">No records found</td></tr>
                  ) : (
                    auditLogs.map((log) => (
                      <tr key={log.id} className="hover:bg-slate-800/40">
                        <td className="py-2 px-3 text-slate-300 font-bold">{log.eventId?.substring(0, 14)}</td>
                        <td className="py-2 px-3 text-indigo-400">{log.eventType}</td>
                        <td className="py-2 px-3">
                          <span className={`px-1.5 py-0.5 rounded text-[9px] font-bold border ${
                            log.status === 'PROCESSED' ? 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20' :
                            log.status === 'RATE_LIMITED' ? 'bg-amber-500/10 text-amber-400 border-amber-500/20' :
                            'bg-cyan-500/10 text-cyan-400 border-cyan-500/20'
                          }`}>
                            {log.status}
                          </span>
                        </td>
                        <td className="py-2 px-3 text-slate-400">{log.partitionNumber ?? '-'}</td>
                        <td className="py-2 px-3 text-slate-400">{log.processingLatencyMs || 0}ms</td>
                        <td className="py-2 px-3 text-slate-500">{(log.createdAt || '').substring(11, 19)}</td>
                        <td className="py-2 px-3 text-right">
                          <button onClick={() => setSelectedPayload(log.payload)} className="text-indigo-400 hover:text-indigo-300">
                            Payload
                          </button>
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </div>

        </div>

      </main>

      {/* Publish Modal */}
      {isModalOpen && (
        <div className="fixed inset-0 bg-slate-950/80 backdrop-blur-sm z-50 flex items-center justify-center p-4">
          <div className="bg-slate-900 border border-slate-800 rounded-2xl max-w-md w-full p-6 shadow-2xl space-y-4">
            <h3 className="text-sm font-bold text-white flex items-center gap-2">
              <Send className="w-4 h-4 text-indigo-400" /> Ingest Test Event
            </h3>
            <form onSubmit={handlePublish} className="space-y-3 text-xs">
              <div>
                <label className="block text-slate-300 mb-1">Event Type</label>
                <select value={formType} onChange={e => setFormType(e.target.value)} className="w-full bg-slate-950 border border-slate-700 rounded-lg p-2 text-white">
                  <option value="TRANSACTION_SETTLED">TRANSACTION_SETTLED</option>
                  <option value="POLICY_RENEWAL">POLICY_RENEWAL</option>
                  <option value="SECURITY_ALERT">SECURITY_ALERT</option>
                  <option value="KYC_VERIFICATION">KYC_VERIFICATION</option>
                </select>
              </div>
              <div>
                <label className="block text-slate-300 mb-1">Idempotency Key (Optional)</label>
                <input type="text" value={formIdempotency} onChange={e => setFormIdempotency(e.target.value)} placeholder="e.g. txn-unique-9921" className="w-full bg-slate-950 border border-slate-700 rounded-lg p-2 text-white" />
              </div>
              <div>
                <label className="block text-slate-300 mb-1">JSON Payload</label>
                <textarea rows="4" value={formPayload} onChange={e => setFormPayload(e.target.value)} className="w-full bg-slate-950 border border-slate-700 rounded-lg p-2 text-white font-mono text-[11px]"></textarea>
              </div>
              <div className="flex justify-end gap-2 pt-2">
                <button type="button" onClick={() => setIsModalOpen(false)} className="px-3 py-1.5 bg-slate-800 text-slate-300 rounded-lg text-xs">Cancel</button>
                <button type="submit" className="px-4 py-1.5 bg-indigo-600 text-white font-bold rounded-lg text-xs">Publish</button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* Payload Viewer Modal */}
      {selectedPayload && (
        <div className="fixed inset-0 bg-slate-950/80 backdrop-blur-sm z-50 flex items-center justify-center p-4">
          <div className="bg-slate-900 border border-slate-800 rounded-2xl max-w-lg w-full p-6 shadow-2xl space-y-4">
            <h3 className="text-sm font-bold text-white flex items-center gap-2">
              <Database className="w-4 h-4 text-indigo-400" /> Event JSON Payload
            </h3>
            <pre className="bg-slate-950 p-4 rounded-xl text-xs font-mono text-emerald-400 overflow-x-auto max-h-80 border border-slate-800">
              {(() => {
                try {
                  return JSON.stringify(JSON.parse(selectedPayload), null, 2);
                } catch (e) {
                  return selectedPayload;
                }
              })()}
            </pre>
            <div className="flex justify-end">
              <button onClick={() => setSelectedPayload(null)} className="px-4 py-1.5 bg-slate-800 text-slate-300 rounded-lg text-xs">Close</button>
            </div>
          </div>
        </div>
      )}

    </div>
  );
}
