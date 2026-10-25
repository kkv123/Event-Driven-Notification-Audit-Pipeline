import React, { useState, useEffect, useCallback, useRef } from 'react';
import {
  Zap,
  Radio,
  Send,
  Flame,
  Settings,
  Database,
  Search,
  CheckCircle2,
  AlertTriangle,
  RotateCcw,
  Award,
  ChevronLeft,
  ChevronRight,
  Code2,
  Trash2
} from 'lucide-react';

import ArchitectureFlow from './components/ArchitectureFlow';
import MetricsCards from './components/MetricsCards';
import TokenBucketGauge from './components/TokenBucketGauge';
import EventTypeChart from './components/EventTypeChart';
import PublishModal from './components/PublishModal';
import PayloadModal from './components/PayloadModal';
import ConfigModal from './components/ConfigModal';

import {
  fetchStats,
  fetchAuditLogs,
  publishEvent,
  triggerBurst,
  purgeAuditLogs,
  subscribeToEventStream
} from './services/api';

export default function App() {
  // Pipeline Stats State
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

  // Live Stream Feed State
  const [liveEvents, setLiveEvents] = useState([]);
  const [streamStatus, setStreamStatus] = useState('Connecting'); // 'Connected' | 'Reconnecting' | 'Connecting'

  // PostgreSQL Audit Logs State
  const [auditData, setAuditData] = useState({
    content: [],
    totalPages: 1,
    number: 0,
    first: true,
    last: true,
    totalElements: 0
  });
  const [currentPage, setCurrentPage] = useState(0);
  const [searchQuery, setSearchQuery] = useState('');
  const [statusFilter, setStatusFilter] = useState('');
  const [auditLoading, setAuditLoading] = useState(false);

  // Modals & Action States
  const [isPublishModalOpen, setIsPublishModalOpen] = useState(false);
  const [isConfigModalOpen, setIsConfigModalOpen] = useState(false);
  const [selectedPayload, setSelectedPayload] = useState(null);
  const [isBursting, setIsBursting] = useState(false);
  const [isPurging, setIsPurging] = useState(false);
  const [streamFilter, setStreamFilter] = useState('ALL');

  // Load Stats
  const loadStats = useCallback(async (sync = false) => {
    try {
      const data = await fetchStats(sync);
      if (data) setStats(data);
    } catch (e) {
      // Backend may be starting up
    }
  }, []);

  // Load Audit Logs
  const loadAudit = useCallback(
    async (page = 0, query = searchQuery, status = statusFilter) => {
      setAuditLoading(true);
      try {
        const res = await fetchAuditLogs({
          page,
          size: 10,
          query,
          status
        });
        setAuditData(res);
        setCurrentPage(page);
      } catch (err) {
        console.error('Audit load error', err);
      } finally {
        setAuditLoading(false);
      }
    },
    [searchQuery, statusFilter]
  );

  // Reference for temporary burst polling timer
  const burstIntervalRef = useRef(null);

  // Clean up any active timer on unmount
  useEffect(() => {
    return () => {
      if (burstIntervalRef.current) {
        clearInterval(burstIntervalRef.current);
      }
    };
  }, []);

  // Initial Data on Page Load / Refresh (Syncs RAM with Supabase DB)
  useEffect(() => {
    loadStats(true);
    loadAudit(0);
  }, [loadStats, loadAudit]);

  // Subscribe to SSE Feed
  useEffect(() => {
    let lastSseStatsRefresh = 0;
    const unsubscribe = subscribeToEventStream(
      (newEvent) => {
        setLiveEvents((prev) => [newEvent, ...prev.slice(0, 39)]);
        // Throttle stats fetch on live events so rapid bursts don't spam requests
        const now = Date.now();
        if (now - lastSseStatsRefresh > 2000) {
          lastSseStatsRefresh = now;
          loadStats();
        }
      },
      () => {
        setStreamStatus('Reconnecting');
      },
      () => {
        setStreamStatus('Connected');
      }
    );

    return () => {
      unsubscribe();
    };
  }, [loadStats]);

  // Handle Event Publish
  const handlePublishEvent = async (payload) => {
    const res = await publishEvent(payload);
    loadStats();
    loadAudit(0);
    return res;
  };

  // Handle Traffic Burst Simulation
  // Non-blocking trigger with immediate 1s interval polling to animate metrics and Token Bucket live
  const handleBurstSimulation = async () => {
    setIsBursting(true);
    if (burstIntervalRef.current) {
      clearInterval(burstIntervalRef.current);
      burstIntervalRef.current = null;
    }

    try {
      // Immediate load
      loadStats();

      // Poll stats every 1 second for 6 seconds to animate the Token Bucket emptying & refilling live
      let pollTicks = 0;
      burstIntervalRef.current = setInterval(() => {
        pollTicks++;
        loadStats();
        if (pollTicks >= 6) {
          clearInterval(burstIntervalRef.current);
          burstIntervalRef.current = null;
          setIsBursting(false);
          loadStats(true);
          loadAudit(0);
        }
      }, 1000);

      // Trigger burst asynchronously without awaiting or blocking the UI thread
      triggerBurst({ count: 35, includeDuplicates: true })
        .then(() => {
          loadStats();
        })
        .catch((err) => {
          console.error('Burst error', err);
          setIsBursting(false);
        });
    } catch (err) {
      console.error('Burst error', err);
      setIsBursting(false);
    }
  };

  // Handle Purge Database Records (Free Tier Storage Guard)
  const handlePurgeDatabase = async () => {
    const confirmed = window.confirm(
      'Are you sure you want to clean up all audit records from Supabase database?\n\nThis will purge historical records and reset counters so your free tier storage does not get filled.'
    );
    if (!confirmed) return;

    setIsPurging(true);
    try {
      const res = await purgeAuditLogs();
      setLiveEvents([]);
      await loadStats(true);
      await loadAudit(0);
      alert(`Cleanup Complete: ${res.recordsPurged ?? 0} historical records purged from Supabase.`);
    } catch (err) {
      console.error('Purge error', err);
      alert('Failed to purge database records: ' + (err.message || 'Check backend connection'));
    } finally {
      setIsPurging(false);
    }
  };

  const handleSearchSubmit = (e) => {
    e.preventDefault();
    loadAudit(0, searchQuery, statusFilter);
  };

  const filteredLiveEvents = liveEvents.filter((evt) => {
    if (streamFilter === 'ALL') return true;
    return evt.status === streamFilter;
  });

  return (
    <div className="bg-slate-950 text-slate-100 font-sans min-h-screen flex flex-col">
      {/* Header */}
      <header className="border-b border-slate-800 bg-slate-900/80 backdrop-blur sticky top-0 z-40 px-6 py-4">
        <div className="max-w-7xl mx-auto flex flex-wrap justify-between items-center gap-4">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-xl bg-gradient-to-tr from-indigo-600 via-sky-500 to-emerald-400 flex items-center justify-center text-white shadow-lg shadow-indigo-500/20">
              <Zap className="w-5 h-5 fill-current" />
            </div>
            <div>
              <h1 className="font-bold text-base sm:text-lg text-white tracking-tight flex items-center gap-2">
                Event-Driven Notification & Audit Pipeline
                <span className="text-[10px] font-semibold px-2 py-0.5 rounded-full bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">
                  Active
                </span>
              </h1>
              <p className="text-xs text-slate-400">
                Java 21 • Spring Boot 3.3 • Apache Kafka • Redis Token Bucket • PostgreSQL
              </p>
            </div>
          </div>

          {/* Quick Badges & Controls */}
          <div className="flex items-center gap-2.5 flex-wrap">
            <div className="flex items-center gap-2 bg-slate-800/80 px-3 py-1.5 rounded-lg border border-slate-700 text-xs">
              <span
                className={`w-2 h-2 rounded-full ${
                  streamStatus === 'Connected'
                    ? 'bg-emerald-400 animate-ping'
                    : 'bg-amber-400 animate-pulse'
                }`}
              />
              <span className="text-slate-300">
                SSE Stream:{' '}
                <span
                  className={`font-semibold ${
                    streamStatus === 'Connected' ? 'text-emerald-400' : 'text-amber-400'
                  }`}
                >
                  {streamStatus}
                </span>
              </span>
            </div>

            <button
              onClick={() => setIsConfigModalOpen(true)}
              title="Configure API Endpoint"
              className="p-2 bg-slate-800 hover:bg-slate-700 text-slate-300 hover:text-white rounded-lg border border-slate-700 text-xs transition"
            >
              <Settings className="w-4 h-4" />
            </button>

            <button
              onClick={() => setIsPublishModalOpen(true)}
              className="px-3.5 py-2 bg-indigo-600 hover:bg-indigo-500 text-white font-medium text-xs rounded-lg transition shadow-md shadow-indigo-600/30 flex items-center gap-1.5"
            >
              <Send className="w-3.5 h-3.5" /> Publish Event
            </button>

            <button
              onClick={handleBurstSimulation}
              disabled={isBursting}
              className="px-3.5 py-2 bg-amber-600 hover:bg-amber-500 text-white font-medium text-xs rounded-lg transition shadow-md shadow-amber-600/30 flex items-center gap-1.5 disabled:opacity-50"
            >
              <Flame className="w-3.5 h-3.5" />
              {isBursting ? 'Burst in Progress...' : 'Simulate Traffic Burst (35 Reqs)'}
            </button>

            <button
              onClick={handlePurgeDatabase}
              disabled={isPurging}
              className="px-3 py-2 bg-rose-950/50 hover:bg-rose-900/70 text-rose-300 border border-rose-800/60 font-medium text-xs rounded-lg transition shadow-sm flex items-center gap-1.5 disabled:opacity-50"
              title="Purge all audit records from Supabase PostgreSQL to free up cloud DB storage"
            >
              <Trash2 className="w-3.5 h-3.5 text-rose-400" />
              {isPurging ? 'Cleaning...' : 'Clean DB'}
            </button>
          </div>
        </div>
      </header>

      {/* Main Container */}
      <main className="max-w-7xl mx-auto px-6 py-6 flex-1 w-full space-y-6">
        {/* 1. Architecture Flow Visualizer */}
        <ArchitectureFlow />

        {/* 2. Top Metrics Grid */}
        <MetricsCards stats={stats} />

        {/* 3. Live Event Stream, Audit Logs, and Analytics Grid */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Left 2 Columns: Live SSE Feed & Audit Logs */}
          <div className="lg:col-span-2 space-y-6">
            {/* Live Stream Panel */}
            <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-xl">
              <div className="flex flex-wrap justify-between items-center mb-3 gap-2">
                <h2 className="text-xs font-semibold text-slate-200 uppercase tracking-wider flex items-center gap-2">
                  <span className="w-2 h-2 rounded-full bg-emerald-400 animate-pulse" />
                  Live Pipeline Stream (SSE)
                </h2>

                <div className="flex items-center gap-2">
                  {/* Status filter tabs for feed */}
                  <div className="flex bg-slate-950 p-0.5 rounded-lg border border-slate-800 text-[10px] font-mono">
                    {['ALL', 'PROCESSED', 'RATE_LIMITED', 'DUPLICATE_IGNORED'].map((tab) => (
                      <button
                        key={tab}
                        onClick={() => setStreamFilter(tab)}
                        className={`px-2 py-0.5 rounded transition ${
                          streamFilter === tab
                            ? 'bg-indigo-600 text-white font-bold'
                            : 'text-slate-400 hover:text-slate-200'
                        }`}
                      >
                        {tab === 'ALL'
                          ? 'ALL'
                          : tab === 'PROCESSED'
                          ? 'OK'
                          : tab === 'RATE_LIMITED'
                          ? '429'
                          : 'DEDUP'}
                      </button>
                    ))}
                  </div>

                  <button
                    onClick={() => setLiveEvents([])}
                    className="text-xs text-slate-500 hover:text-slate-300 transition"
                  >
                    Clear Feed
                  </button>
                </div>
              </div>

              <div className="space-y-2 max-h-[360px] overflow-y-auto custom-scrollbar pr-1">
                {filteredLiveEvents.length === 0 ? (
                  <div className="text-center py-12 text-slate-500 text-xs">
                    <Radio className="w-8 h-8 mx-auto mb-2 text-slate-600 stroke-[1.5]" />
                    Listening for events... Click <strong>"Publish Event"</strong> or{' '}
                    <strong>"Simulate Traffic Burst"</strong> to test!
                  </div>
                ) : (
                  filteredLiveEvents.map((evt, idx) => {
                    let badgeBg = 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20';
                    let statusLabel = evt.status || 'PROCESSED';
                    let IconComponent = CheckCircle2;

                    if (evt.status === 'RATE_LIMITED') {
                      badgeBg = 'bg-amber-500/10 text-amber-400 border-amber-500/20';
                      IconComponent = AlertTriangle;
                    } else if (evt.status === 'DUPLICATE_IGNORED') {
                      badgeBg = 'bg-cyan-500/10 text-cyan-400 border-cyan-500/20';
                      IconComponent = RotateCcw;
                    } else if (evt.status === 'FAILED' || evt.status === 'DLQ') {
                      badgeBg = 'bg-rose-500/10 text-rose-400 border-rose-500/20';
                      IconComponent = AlertTriangle;
                    }

                    return (
                      <div
                        key={evt.eventId || idx}
                        className="bg-slate-950 border border-slate-800/80 rounded-xl p-3 text-xs flex justify-between items-center transition hover:border-slate-700"
                      >
                        <div className="flex items-center gap-3 truncate pr-2">
                          <div
                            className={`w-7 h-7 rounded-lg flex items-center justify-center text-xs flex-shrink-0 border ${badgeBg}`}
                          >
                            <IconComponent className="w-3.5 h-3.5" />
                          </div>
                          <div className="truncate">
                            <div className="font-bold text-slate-200 font-mono text-xs truncate">
                              {evt.eventId}
                            </div>
                            <div className="text-[11px] text-slate-400 flex items-center gap-2 mt-0.5 truncate">
                              <span className="text-indigo-400 font-semibold">{evt.eventType}</span>
                              <span>•</span>
                              <span className="truncate">{evt.sourceService}</span>
                              {evt.partitionNumber !== null && evt.partitionNumber !== undefined && (
                                <span>• Part: {evt.partitionNumber}</span>
                              )}
                            </div>
                          </div>
                        </div>

                        <div className="text-right flex-shrink-0">
                          <span className={`px-2 py-0.5 rounded text-[10px] font-bold border ${badgeBg}`}>
                            {statusLabel}
                          </span>
                          <div className="text-[10px] text-slate-500 mt-1 font-mono">
                            {evt.processingLatencyMs || 0}ms
                          </div>
                        </div>
                      </div>
                    );
                  })
                )}
              </div>
            </div>

            {/* PostgreSQL Historical Audit Logs Table */}
            <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-xl">
              <div className="flex flex-wrap justify-between items-center gap-3 mb-4">
                <h2 className="text-xs font-semibold text-slate-200 uppercase tracking-wider flex items-center gap-2">
                  <Database className="w-4 h-4 text-blue-400" /> PostgreSQL Audit Log Search
                </h2>

                <form onSubmit={handleSearchSubmit} className="flex flex-wrap gap-2">
                  <input
                    type="text"
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    placeholder="Search Event ID, Type, Payload..."
                    className="bg-slate-950 border border-slate-700 rounded-lg px-3 py-1.5 text-xs text-white placeholder-slate-500 focus:outline-none focus:border-indigo-500 w-48 sm:w-56"
                  />
                  <select
                    value={statusFilter}
                    onChange={(e) => {
                      setStatusFilter(e.target.value);
                      loadAudit(0, searchQuery, e.target.value);
                    }}
                    className="bg-slate-950 border border-slate-700 rounded-lg px-2 py-1.5 text-xs text-white focus:outline-none focus:border-indigo-500"
                  >
                    <option value="">All Statuses</option>
                    <option value="PROCESSED">PROCESSED</option>
                    <option value="RATE_LIMITED">RATE_LIMITED</option>
                    <option value="DUPLICATE_IGNORED">DUPLICATE_IGNORED</option>
                  </select>
                  <button
                    type="submit"
                    className="bg-slate-800 hover:bg-slate-700 text-slate-200 px-3 py-1.5 rounded-lg text-xs font-medium transition flex items-center justify-center"
                    title="Search"
                  >
                    <Search className="w-3.5 h-3.5" />
                  </button>

                  <button
                    type="button"
                    onClick={handlePurgeDatabase}
                    disabled={isPurging}
                    className="bg-rose-950/40 hover:bg-rose-900/60 text-rose-300 border border-rose-800/60 px-2.5 py-1.5 rounded-lg text-xs font-medium transition flex items-center gap-1.5 disabled:opacity-50"
                    title="Purge all audit logs in Supabase to free up cloud database storage"
                  >
                    <Trash2 className="w-3.5 h-3.5 text-rose-400" />
                    <span className="hidden sm:inline">{isPurging ? 'Purging...' : 'Purge DB'}</span>
                  </button>
                </form>
              </div>

              {/* Table */}
              <div className="overflow-x-auto">
                <table className="w-full text-left text-xs text-slate-300">
                  <thead className="bg-slate-950/70 text-slate-400 uppercase font-mono text-[10px] border-b border-slate-800">
                    <tr>
                      <th className="py-2.5 px-3">Event ID</th>
                      <th className="py-2.5 px-3">Type</th>
                      <th className="py-2.5 px-3">Service</th>
                      <th className="py-2.5 px-3">Status</th>
                      <th className="py-2.5 px-3">Partition</th>
                      <th className="py-2.5 px-3">Latency</th>
                      <th className="py-2.5 px-3">Time</th>
                      <th className="py-2.5 px-3 text-right">Payload</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-800/60 font-mono text-[11px]">
                    {auditLoading ? (
                      <tr>
                        <td colSpan="8" className="text-center py-8 text-slate-500">
                          Loading audit records...
                        </td>
                      </tr>
                    ) : !auditData.content || auditData.content.length === 0 ? (
                      <tr>
                        <td colSpan="8" className="text-center py-8 text-slate-500">
                          No audit records found matching criteria
                        </td>
                      </tr>
                    ) : (
                      auditData.content.map((evt) => {
                        let badgeBg = 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20';
                        if (evt.status === 'RATE_LIMITED')
                          badgeBg = 'bg-amber-500/10 text-amber-400 border-amber-500/20';
                        else if (evt.status === 'DUPLICATE_IGNORED')
                          badgeBg = 'bg-cyan-500/10 text-cyan-400 border-cyan-500/20';

                        return (
                          <tr key={evt.id || evt.eventId} className="hover:bg-slate-800/40 transition">
                            <td className="py-2 px-3 text-slate-300 font-bold">
                              {(evt.eventId || '').substring(0, 16)}
                            </td>
                            <td className="py-2 px-3 text-indigo-400">{evt.eventType}</td>
                            <td className="py-2 px-3 text-slate-400">{evt.sourceService}</td>
                            <td className="py-2 px-3">
                              <span className={`px-1.5 py-0.5 rounded text-[10px] font-bold border ${badgeBg}`}>
                                {evt.status}
                              </span>
                            </td>
                            <td className="py-2 px-3 text-slate-400">
                              {evt.partitionNumber !== null && evt.partitionNumber !== undefined
                                ? evt.partitionNumber
                                : '-'}
                            </td>
                            <td className="py-2 px-3 text-slate-400">
                              {evt.processingLatencyMs || 0}ms
                            </td>
                            <td className="py-2 px-3 text-slate-500">
                              {(evt.createdAt || '').replace('T', ' ').substring(11, 19)}
                            </td>
                            <td className="py-2 px-3 text-right">
                              <button
                                onClick={() => setSelectedPayload(evt.payload || '{}')}
                                className="text-indigo-400 hover:text-indigo-300 transition p-1"
                                title="View Raw JSON"
                              >
                                <Code2 className="w-3.5 h-3.5" />
                              </button>
                            </td>
                          </tr>
                        );
                      })
                    )}
                  </tbody>
                </table>
              </div>

              {/* Pagination */}
              <div className="flex justify-between items-center mt-3 pt-3 border-t border-slate-800 text-xs text-slate-400">
                <div>
                  Showing page{' '}
                  <span className="text-white font-medium">{currentPage + 1}</span> of{' '}
                  <span className="text-white font-medium">{auditData.totalPages || 1}</span>{' '}
                  ({auditData.totalElements ?? 0} total)
                </div>
                <div className="flex gap-2">
                  <button
                    onClick={() => loadAudit(currentPage - 1)}
                    disabled={auditData.first || currentPage === 0}
                    className="px-3 py-1 rounded bg-slate-800 hover:bg-slate-700 text-slate-200 disabled:opacity-40 disabled:cursor-not-allowed transition flex items-center gap-1"
                  >
                    <ChevronLeft className="w-3.5 h-3.5" /> Prev
                  </button>
                  <button
                    onClick={() => loadAudit(currentPage + 1)}
                    disabled={auditData.last || currentPage + 1 >= auditData.totalPages}
                    className="px-3 py-1 rounded bg-slate-800 hover:bg-slate-700 text-slate-200 disabled:opacity-40 disabled:cursor-not-allowed transition flex items-center gap-1"
                  >
                    Next <ChevronRight className="w-3.5 h-3.5" />
                  </button>
                </div>
              </div>
            </div>
          </div>

          {/* Right 1 Column: Live Metrics, Token Bucket Gauge, and Charts */}
          <div className="space-y-6">
            {/* Token Bucket Gauge */}
            <TokenBucketGauge
              available={stats.tokenBucketAvailable}
              capacity={stats.tokenBucketCapacity}
            />

            {/* Event Type Distribution Donut Chart */}
            <EventTypeChart eventsByType={stats.eventsByType} />

            {/* Architectural Interview Highlights */}
            <div className="bg-gradient-to-br from-indigo-950/40 via-slate-900 to-slate-950 border border-indigo-900/40 rounded-2xl p-5 shadow-xl">
              <h2 className="text-xs font-bold text-indigo-300 uppercase tracking-wider mb-2.5 flex items-center gap-2">
                <Award className="w-4 h-4 text-indigo-400" /> Architectural Highlights
              </h2>
              <ul className="text-xs text-slate-300 space-y-2.5 leading-relaxed">
                <li className="flex items-start gap-2">
                  <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 mt-1.5 flex-shrink-0" />
                  <span>
                    <strong>Guaranteed At-Least-Once:</strong> Manual offset commit (
                    <code className="text-indigo-300 font-mono text-[10px]">MANUAL_IMMEDIATE</code>
                    ) committed strictly after PostgreSQL audit transaction succeeds.
                  </span>
                </li>
                <li className="flex items-start gap-2">
                  <span className="w-1.5 h-1.5 rounded-full bg-amber-400 mt-1.5 flex-shrink-0" />
                  <span>
                    <strong>Token Bucket Algorithm:</strong> Distributed atomic rate limiting
                    protects downstream databases and Kafka brokers from high-burst spikes.
                  </span>
                </li>
                <li className="flex items-start gap-2">
                  <span className="w-1.5 h-1.5 rounded-full bg-cyan-400 mt-1.5 flex-shrink-0" />
                  <span>
                    <strong>Redis Deduplication:</strong> Cryptographic SHA-256 fingerprinting
                    guarantees idempotency and eliminates network retry duplicates.
                  </span>
                </li>
              </ul>
            </div>
          </div>
        </div>
      </main>

      {/* Modals */}
      <PublishModal
        isOpen={isPublishModalOpen}
        onClose={() => setIsPublishModalOpen(false)}
        onPublished={handlePublishEvent}
      />

      <PayloadModal
        payload={selectedPayload}
        onClose={() => setSelectedPayload(null)}
      />

      <ConfigModal
        isOpen={isConfigModalOpen}
        onClose={() => setIsConfigModalOpen(false)}
        onConfigSaved={() => {
          loadStats();
          loadAudit(0);
        }}
      />
    </div>
  );
}
