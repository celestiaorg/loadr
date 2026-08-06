/* loadr chart.js — hand-rolled canvas time-series charts. No dependencies. */
'use strict';

(function () {
  function cssVar(name, fallback) {
    const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
    return v || fallback;
  }

  // Pick a "nice" step size so axis labels land on round numbers.
  function niceStep(range, targetTicks) {
    if (range <= 0) return 1;
    const rough = range / Math.max(1, targetTicks);
    const mag = Math.pow(10, Math.floor(Math.log10(rough)));
    const norm = rough / mag;
    let step;
    if (norm <= 1) step = 1;
    else if (norm <= 2) step = 2;
    else if (norm <= 5) step = 5;
    else step = 10;
    return step * mag;
  }

  function defaultFormat(v) {
    if (v == null || !isFinite(v)) return '–';
    const abs = Math.abs(v);
    if (abs >= 1e9) return (v / 1e9).toFixed(1) + 'B';
    if (abs >= 1e6) return (v / 1e6).toFixed(1) + 'M';
    if (abs >= 1e3) return (v / 1e3).toFixed(1) + 'k';
    if (abs >= 100) return v.toFixed(0);
    if (abs >= 1) return v.toFixed(1);
    if (abs === 0) return '0';
    return v.toFixed(2);
  }

  function timeLabel(ms) {
    const d = new Date(ms);
    const p = (n) => String(n).padStart(2, '0');
    return p(d.getHours()) + ':' + p(d.getMinutes()) + ':' + p(d.getSeconds());
  }

  class TimeChart {
    /**
     * @param {HTMLCanvasElement} canvas
     * @param {{series: {label: string, color: string, fill?: boolean}[],
     *          window?: number, format?: (v:number)=>string, min0?: boolean}} opts
     */
    constructor(canvas, opts = {}) {
      this.canvas = canvas;
      this.ctx = canvas.getContext('2d');
      this.series = (opts.series || []).map((s) => ({
        label: s.label,
        color: s.color,
        fill: !!s.fill,
        points: [], // [tMs, value|null]
      }));
      this.window = opts.window || 180; // points kept per series
      this.format = opts.format || defaultFormat;
      this.min0 = opts.min0 !== false;
      // Render style: 'line', 'area' (line + fill) or 'bars'.
      this.type = opts.type || 'line';
      this._onResize = () => this.draw();
      window.addEventListener('resize', this._onResize);
      this.draw();
    }

    /** Switch render style ('line' | 'area' | 'bars') and redraw. */
    setType(type) {
      this.type = type;
      this.draw();
    }

    destroy() {
      window.removeEventListener('resize', this._onResize);
    }

    /** Append one value per series at time t (ms) and redraw. */
    push(t, values) {
      this.series.forEach((s, i) => {
        const v = values[i];
        s.points.push([t, v == null || !isFinite(v) ? null : v]);
        if (s.points.length > this.window) s.points.shift();
      });
      this.draw();
    }

    clear() {
      this.series.forEach((s) => (s.points = []));
      this.draw();
    }

    draw() {
      const canvas = this.canvas;
      if (!canvas.isConnected) return;
      const dpr = window.devicePixelRatio || 1;
      const cssW = canvas.clientWidth || canvas.parentElement?.clientWidth || 600;
      const cssH = canvas.clientHeight || 220;
      if (canvas.width !== Math.round(cssW * dpr) || canvas.height !== Math.round(cssH * dpr)) {
        canvas.width = Math.round(cssW * dpr);
        canvas.height = Math.round(cssH * dpr);
      }
      const ctx = this.ctx;
      ctx.save();
      ctx.scale(dpr, dpr);
      ctx.clearRect(0, 0, cssW, cssH);

      const colText = cssVar('--muted', '#8b949e');
      const colGrid = cssVar('--chart-grid', 'rgba(128,128,128,0.15)');
      const padL = 52;
      const padR = 12;
      const padT = 26;
      const padB = 22;
      const w = cssW - padL - padR;
      const h = cssH - padT - padB;
      if (w < 20 || h < 20) {
        ctx.restore();
        return;
      }

      // Extents.
      let tMin = Infinity;
      let tMax = -Infinity;
      let vMin = this.min0 ? 0 : Infinity;
      let vMax = -Infinity;
      let hasData = false;
      for (const s of this.series) {
        for (const [t, v] of s.points) {
          if (t < tMin) tMin = t;
          if (t > tMax) tMax = t;
          if (v != null) {
            hasData = true;
            if (v < vMin) vMin = v;
            if (v > vMax) vMax = v;
          }
        }
      }
      if (!hasData) {
        vMin = 0;
        vMax = 1;
      }
      if (vMax <= vMin) vMax = vMin + 1;
      // Headroom so lines never touch the top.
      vMax += (vMax - vMin) * 0.08;
      if (tMax <= tMin) tMax = tMin + 1000;

      const x = (t) => padL + ((t - tMin) / (tMax - tMin)) * w;
      const y = (v) => padT + h - ((v - vMin) / (vMax - vMin)) * h;

      // Y grid + labels.
      ctx.font = '10px ui-monospace, SFMono-Regular, Menlo, monospace';
      ctx.fillStyle = colText;
      ctx.strokeStyle = colGrid;
      ctx.lineWidth = 1;
      const step = niceStep(vMax - vMin, 4);
      const yStart = Math.ceil(vMin / step) * step;
      for (let v = yStart; v <= vMax + 1e-9; v += step) {
        const yy = Math.round(y(v)) + 0.5;
        ctx.beginPath();
        ctx.moveTo(padL, yy);
        ctx.lineTo(padL + w, yy);
        ctx.stroke();
        ctx.textAlign = 'right';
        ctx.textBaseline = 'middle';
        ctx.fillText(this.format(v), padL - 6, yy);
      }

      // X labels (start / middle / end).
      if (hasData) {
        ctx.textAlign = 'center';
        ctx.textBaseline = 'top';
        const labels = tMax - tMin > 4000 ? 3 : 1;
        for (let i = 0; i < labels; i++) {
          const t = tMin + ((tMax - tMin) * i) / Math.max(1, labels - 1 || 1);
          const tt = labels === 1 ? tMax : t;
          ctx.fillText(timeLabel(tt), x(tt), padT + h + 6);
        }
      }

      // Series.
      const baseY = padT + h;
      if (this.type === 'bars') {
        // Vertical bars per data point. Width derived from the time window so
        // bars stay a consistent thickness as points stream in. With multiple
        // series we draw translucent overlaid bars so each stays visible.
        const slot = w / this.window;
        const barW = Math.max(1.5, slot * 0.78);
        const multi = this.series.length > 1;
        for (const s of this.series) {
          if (!s.points.length) continue;
          ctx.fillStyle = s.color;
          ctx.globalAlpha = multi ? 0.5 : 0.82;
          for (const [t, v] of s.points) {
            if (v == null) continue;
            const py = y(v);
            ctx.fillRect(x(t) - barW / 2, py, barW, Math.max(0.5, baseY - py));
          }
        }
        ctx.globalAlpha = 1;
      } else {
        for (const s of this.series) {
          if (!s.points.length) continue;
          ctx.lineWidth = 1.8;
          ctx.strokeStyle = s.color;
          ctx.lineJoin = 'round';
          let started = false;
          ctx.beginPath();
          for (const [t, v] of s.points) {
            if (v == null) {
              started = false;
              continue;
            }
            const px = x(t);
            const py = y(v);
            if (!started) {
              ctx.moveTo(px, py);
              started = true;
            } else {
              ctx.lineTo(px, py);
            }
          }
          ctx.stroke();

          // 'area' fills every series; 'line' only fills series opting in.
          if (this.type === 'area' || s.fill) {
            const pts = s.points.filter((p) => p[1] != null);
            if (pts.length > 1) {
              ctx.beginPath();
              ctx.moveTo(x(pts[0][0]), y(pts[0][1]));
              for (const [t, v] of pts) ctx.lineTo(x(t), y(v));
              ctx.lineTo(x(pts[pts.length - 1][0]), baseY);
              ctx.lineTo(x(pts[0][0]), baseY);
              ctx.closePath();
              ctx.globalAlpha = this.type === 'area' ? 0.16 : 0.12;
              ctx.fillStyle = s.color;
              ctx.fill();
              ctx.globalAlpha = 1;
            }
          }
        }
      }

      // Legend with latest values.
      ctx.textBaseline = 'middle';
      ctx.textAlign = 'left';
      ctx.font = '11px ui-monospace, SFMono-Regular, Menlo, monospace';
      let lx = padL;
      for (const s of this.series) {
        const last = [...s.points].reverse().find((p) => p[1] != null);
        const text = s.label + (last ? ' ' + this.format(last[1]) : '');
        ctx.fillStyle = s.color;
        ctx.beginPath();
        ctx.arc(lx + 4, 12, 3.5, 0, Math.PI * 2);
        ctx.fill();
        ctx.fillStyle = colText;
        ctx.fillText(text, lx + 12, 12);
        lx += 20 + ctx.measureText(text).width;
        if (lx > cssW - 60) break;
      }

      ctx.restore();
    }
  }

  window.TimeChart = TimeChart;
})();
