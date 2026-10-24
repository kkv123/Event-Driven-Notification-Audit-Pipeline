import React, { useState } from 'react';
import { Send, X, AlertCircle } from 'lucide-react';

const PRESETS = {
  TRANSACTION_SETTLED: {
    service: 'payment-gateway-service',
    payload: {
      userId: 'usr_99812',
      amount: 2450.0,
      currency: 'INR',
      status: 'SUCCESS',
      paymentMethod: 'UPI'
    }
  },
  POLICY_RENEWAL: {
    service: 'policy-admin-service',
    payload: {
      policyNumber: 'POL-2026-90812',
      insuredName: 'Aarav Sharma',
      premiumPaid: 18500.0,
      validUntil: '2027-10-31'
    }
  },
  KYC_VERIFICATION: {
    service: 'kyc-compliance-service',
    payload: {
      userId: 'usr_kyc_4412',
      docType: 'PASSPORT',
      verificationScore: 0.98,
      status: 'VERIFIED'
    }
  },
  SECURITY_ALERT: {
    service: 'fraud-detection-engine',
    payload: {
      alertLevel: 'HIGH',
      riskScore: 89,
      clientIp: '198.51.100.24',
      reason: 'Velocity threshold exceeded: 12 attempts in 4s'
    }
  },
  NOTIFICATION: {
    service: 'customer-notify-service',
    payload: {
      channel: 'SMS',
      recipient: '+919876543210',
      templateId: 'OTP_AUTH_MSG',
      status: 'QUEUED'
    }
  }
};

export default function PublishModal({ isOpen, onClose, onPublished }) {
  const [eventType, setEventType] = useState('TRANSACTION_SETTLED');
  const [sourceService, setSourceService] = useState('payment-gateway-service');
  const [idempotencyKey, setIdempotencyKey] = useState('');
  const [jsonText, setJsonText] = useState(
    JSON.stringify(PRESETS.TRANSACTION_SETTLED.payload, null, 2)
  );
  const [errorMsg, setErrorMsg] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);

  if (!isOpen) return null;

  const handleTypeChange = (newType) => {
    setEventType(newType);
    if (PRESETS[newType]) {
      setSourceService(PRESETS[newType].service);
      setJsonText(JSON.stringify(PRESETS[newType].payload, null, 2));
    }
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setErrorMsg('');

    let parsedPayload;
    try {
      parsedPayload = JSON.parse(jsonText);
    } catch (err) {
      setErrorMsg('Invalid JSON format: ' + err.message);
      return;
    }

    setIsSubmitting(true);
    try {
      await onPublished({
        eventType,
        sourceService,
        idempotencyKey: idempotencyKey.trim() || null,
        data: parsedPayload
      });
      onClose();
    } catch (err) {
      setErrorMsg(err.message || 'Failed to publish event');
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="fixed inset-0 bg-slate-950/80 backdrop-blur-sm z-50 flex items-center justify-center p-4">
      <div className="bg-slate-900 border border-slate-800 rounded-2xl max-w-lg w-full p-6 shadow-2xl space-y-4">
        <div className="flex justify-between items-center border-b border-slate-800 pb-3">
          <h3 className="text-sm font-bold text-white flex items-center gap-2">
            <Send className="w-4 h-4 text-indigo-400" /> Ingest Test Event
          </h3>
          <button
            onClick={onClose}
            className="text-slate-400 hover:text-white transition"
          >
            <X className="w-4 h-4" />
          </button>
        </div>

        {errorMsg && (
          <div className="bg-rose-500/10 border border-rose-500/20 text-rose-400 text-xs p-3 rounded-lg flex items-center gap-2">
            <AlertCircle className="w-4 h-4 flex-shrink-0" />
            <span>{errorMsg}</span>
          </div>
        )}

        <form onSubmit={handleSubmit} className="space-y-3 text-xs">
          <div>
            <label className="block text-slate-300 mb-1 font-medium">Event Type</label>
            <select
              value={eventType}
              onChange={(e) => handleTypeChange(e.target.value)}
              className="w-full bg-slate-950 border border-slate-700 rounded-lg p-2.5 text-white focus:outline-none focus:border-indigo-500"
            >
              <option value="TRANSACTION_SETTLED">TRANSACTION_SETTLED</option>
              <option value="POLICY_RENEWAL">POLICY_RENEWAL</option>
              <option value="KYC_VERIFICATION">KYC_VERIFICATION</option>
              <option value="SECURITY_ALERT">SECURITY_ALERT</option>
              <option value="NOTIFICATION">NOTIFICATION</option>
            </select>
          </div>

          <div>
            <label className="block text-slate-300 mb-1 font-medium">Source Microservice</label>
            <input
              type="text"
              value={sourceService}
              onChange={(e) => setSourceService(e.target.value)}
              required
              className="w-full bg-slate-950 border border-slate-700 rounded-lg p-2.5 text-white focus:outline-none focus:border-indigo-500 font-mono text-[11px]"
            />
          </div>

          <div>
            <label className="block text-slate-300 mb-1 font-medium">
              Idempotency Key (Optional - tests Redis SHA-256 deduplication)
            </label>
            <input
              type="text"
              value={idempotencyKey}
              onChange={(e) => setIdempotencyKey(e.target.value)}
              placeholder="e.g. order-xyz-12345"
              className="w-full bg-slate-950 border border-slate-700 rounded-lg p-2.5 text-white focus:outline-none focus:border-indigo-500 font-mono text-[11px]"
            />
          </div>

          <div>
            <div className="flex justify-between items-center mb-1">
              <label className="text-slate-300 font-medium">JSON Payload</label>
              <button
                type="button"
                onClick={() => {
                  try {
                    setJsonText(JSON.stringify(JSON.parse(jsonText), null, 2));
                  } catch (e) {}
                }}
                className="text-[10px] text-indigo-400 hover:text-indigo-300"
              >
                Format JSON
              </button>
            </div>
            <textarea
              rows={5}
              value={jsonText}
              onChange={(e) => setJsonText(e.target.value)}
              className="w-full bg-slate-950 border border-slate-700 rounded-lg p-2.5 text-emerald-400 font-mono text-[11px] focus:outline-none focus:border-indigo-500 custom-scrollbar"
            />
          </div>

          <div className="flex justify-end gap-3 pt-3 border-t border-slate-800">
            <button
              type="button"
              onClick={onClose}
              className="px-4 py-2 bg-slate-800 hover:bg-slate-700 text-slate-300 rounded-lg text-xs font-medium transition"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={isSubmitting}
              className="px-4 py-2 bg-indigo-600 hover:bg-indigo-500 text-white rounded-lg text-xs font-bold shadow-md shadow-indigo-600/30 flex items-center gap-1.5 transition disabled:opacity-50"
            >
              <Send className="w-3.5 h-3.5" />
              {isSubmitting ? 'Publishing...' : 'Publish to Pipeline'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
