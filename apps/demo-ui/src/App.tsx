import { FormEvent, useEffect, useState } from 'react';
import { io } from 'socket.io-client';

const API = import.meta.env.VITE_API_URL ?? 'http://localhost:3000';
const KAFKA_UI = 'http://localhost:8080';
const MAILHOG = 'http://localhost:8025';

type Status = 'PENDING' | 'IN_PROGRESS' | 'ACTIVE' | 'FAILED';
type FailureMode = 'none' | 'billing' | 'provisioning';

interface TimelineEvent { eventType: string; source: string; at: string }
interface Activation {
  id: string;
  customerId: string;
  planId: string;
  status: Status;
  events: TimelineEvent[];
}
interface WsMessage extends TimelineEvent {
  activationId: string;
  customerId: string;
  planId: string;
  status: Status;
}

const EVENT_LABEL: Record<string, string> = {
  ActivationRequested: 'Activación solicitada',
  BillingAccountCreated: 'Cuenta de facturación creada',
  BillingFailed: 'Falló la facturación',
  BillingAccountCancelled: 'Cuenta de facturación anulada',
  ProvisioningCompleted: 'Línea aprovisionada',
  ProvisioningFailed: 'Falló el aprovisionamiento',
  ActivationCompleted: 'Activación completada',
  ActivationFailed: 'Activación fallida',
};
const TONE: Record<string, 'ok' | 'bad' | 'warn' | 'neutral'> = {
  BillingAccountCreated: 'ok',
  ProvisioningCompleted: 'ok',
  ActivationCompleted: 'ok',
  BillingFailed: 'bad',
  ProvisioningFailed: 'bad',
  ActivationFailed: 'bad',
  BillingAccountCancelled: 'warn',
};
const STATUS_LABEL: Record<Status, string> = {
  PENDING: 'Pendiente',
  IN_PROGRESS: 'En curso',
  ACTIVE: 'Activa',
  FAILED: 'Fallida',
};
const PLANS = ['PLAN DIGITAL', 'PLAN PREMIUM', 'PLAN BASICO'];

const time = (iso: string) => new Date(iso).toLocaleTimeString('es-AR', { hour12: false });

export default function App() {
  const [items, setItems] = useState<Activation[]>([]);
  const [connected, setConnected] = useState(false);
  const [customerId, setCustomerId] = useState('C-1234');
  const [planId, setPlanId] = useState(PLANS[0]);
  const [failure, setFailure] = useState<FailureMode>('none');
  const [sending, setSending] = useState(false);
  const [error, setError] = useState('');

  // Historial reciente al abrir la página (GET /activations).
  useEffect(() => {
    fetch(`${API}/activations`)
      .then((r) => r.json())
      .then((docs: any[]) =>
        setItems(
          docs.map((d) => ({
            id: d._id,
            customerId: d.customerId,
            planId: d.planId,
            status: d.status,
            events: d.history as TimelineEvent[],
          })),
        ),
      )
      .catch(() => setError('No se pudo cargar el historial: ¿está activation-api levantada?'));
  }, []);

  // Timeline en vivo por WebSocket (RF-08).
  useEffect(() => {
    const socket = io(API);
    socket.on('connect', () => setConnected(true));
    socket.on('disconnect', () => setConnected(false));
    socket.on('activation.event', (m: WsMessage) => {
      setItems((prev) => {
        const ev = { eventType: m.eventType, source: m.source, at: m.at };
        const found = prev.find((a) => a.id === m.activationId);
        if (!found) {
          return [{ id: m.activationId, customerId: m.customerId, planId: m.planId, status: m.status, events: [ev] }, ...prev];
        }
        return prev.map((a) =>
          a.id !== m.activationId
            ? a
            : {
                ...a,
                status: m.status,
                events: a.events.some((e) => e.eventType === ev.eventType && e.at === ev.at) ? a.events : [...a.events, ev],
              },
        );
      });
    });
    return () => {
      socket.close();
    };
  }, []);

  async function submit(e: FormEvent) {
    e.preventDefault();
    setError('');
    setSending(true);
    try {
      const res = await fetch(`${API}/activations`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ customerId, planId, simulateFailure: failure }),
      });
      if (!res.ok) throw new Error((await res.json()).message ?? `HTTP ${res.status}`);
      const { activationId } = await res.json();
      // Si el WebSocket todavía no entregó el primer evento, se muestra igual.
      setItems((prev) =>
        prev.some((a) => a.id === activationId)
          ? prev
          : [{ id: activationId, customerId, planId, status: 'PENDING', events: [] }, ...prev],
      );
    } catch (err) {
      setError(`No se pudo contratar el plan: ${(err as Error).message}`);
    } finally {
      setSending(false);
    }
  }

  return (
    <div className="shell">
      <header className="top">
        <div>
          <h1>Activación de planes</h1>
          <p className="sub">Un evento en Kafka dispara facturación, aprovisionamiento y aviso al cliente.</p>
        </div>
        <nav className="links">
          <span className={`live ${connected ? 'on' : 'off'}`}>{connected ? 'En vivo' : 'Sin conexión'}</span>
          <a href={KAFKA_UI} target="_blank" rel="noreferrer">Kafka UI</a>
          <a href={MAILHOG} target="_blank" rel="noreferrer">Mailhog</a>
        </nav>
      </header>

      <main className="grid">
        <form className="panel form" onSubmit={submit}>
          <h2>Contratar plan</h2>
          <label>
            Cliente
            <input value={customerId} onChange={(e) => setCustomerId(e.target.value)} required />
          </label>
          <label>
            Plan
            <select value={planId} onChange={(e) => setPlanId(e.target.value)}>
              {PLANS.map((p) => (
                <option key={p}>{p}</option>
              ))}
            </select>
          </label>
          <label>
            Forzar un fallo
            <select value={failure} onChange={(e) => setFailure(e.target.value as FailureMode)}>
              <option value="none">Ninguno</option>
              <option value="billing">En facturación</option>
              <option value="provisioning">En aprovisionamiento</option>
            </select>
          </label>
          <button type="submit" disabled={sending}>{sending ? 'Enviando…' : 'Contratar plan'}</button>
          {error && <p className="error" role="alert">{error}</p>}
        </form>

        <section className="list" aria-live="polite">
          {items.length === 0 && (
            <div className="panel empty">
              <h2>Todavía no hay activaciones</h2>
              <p>Contratá un plan para ver cómo cada servicio reacciona al evento, en orden y en tiempo real.</p>
            </div>
          )}
          {items.map((a) => (
            <article key={a.id} className={`panel card status-${a.status}`}>
              <header>
                <div>
                  <h2>{a.id}</h2>
                  <p className="meta">{a.customerId} · {a.planId}</p>
                </div>
                <span className={`badge ${a.status}`}>{STATUS_LABEL[a.status]}</span>
              </header>
              <ol className="timeline">
                {a.events.map((ev, i) => (
                  <li key={`${ev.eventType}-${ev.at}-${i}`} className={TONE[ev.eventType] ?? 'neutral'}>
                    <span className="at">{time(ev.at)}</span>
                    <span className="what">{EVENT_LABEL[ev.eventType] ?? ev.eventType}</span>
                    <span className="who">{ev.source}</span>
                  </li>
                ))}
              </ol>
            </article>
          ))}
        </section>
      </main>
    </div>
  );
}
