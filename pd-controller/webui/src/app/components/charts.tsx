import type { LineSeries } from "@/app/helpers";
import type { EdgeTrafficPoint } from "@/app/types";

export function LineChart({
  points,
  valueFor,
  stroke,
  emptyLabel
}: {
  points: EdgeTrafficPoint[];
  valueFor: (point: EdgeTrafficPoint) => number;
  stroke: string;
  emptyLabel: string;
}) {
  if (points.length === 0) {
    return (
      <div className="h-[160px] rounded-md border bg-background/70 p-3 text-sm text-muted-foreground">
        {emptyLabel}
      </div>
    );
  }

  const width = 520;
  const height = 160;
  const maxY = Math.max(...points.map((point) => valueFor(point)), 1);
  const step = points.length > 1 ? width / (points.length - 1) : 0;
  const path = points
    .map((point, index) => {
      const x = index * step;
      const value = valueFor(point);
      const y = height - (value / maxY) * (height - 10) - 5;
      return `${index === 0 ? "M" : "L"} ${x.toFixed(1)} ${y.toFixed(1)}`;
    })
    .join(" ");

  return (
    <div className="rounded-md border bg-background/70 p-3">
      <svg viewBox={`0 0 ${width} ${height}`} className="h-[160px] w-full">
        <path d={path} fill="none" stroke={stroke} strokeWidth={2.5} />
      </svg>
      <div className="mt-2 text-xs text-muted-foreground">
        latest={valueFor(points[points.length - 1])} max={maxY}
      </div>
    </div>
  );
}

export function MultiLineChart({
  points,
  series,
  emptyLabel,
  hideZeroSeries = false
}: {
  points: EdgeTrafficPoint[];
  series: LineSeries[];
  emptyLabel: string;
  hideZeroSeries?: boolean;
}) {
  const visibleSeries = hideZeroSeries
    ? series.filter((item) => points.some((point) => item.valueFor(point) > 0))
    : series;
  if (points.length === 0 || visibleSeries.length === 0) {
    return (
      <div className="rounded-md border bg-background/70 p-3">
        <svg viewBox="0 0 520 160" className="h-[160px] w-full" aria-label={emptyLabel}>
          <path
            d="M 0 80 L 520 80"
            fill="none"
            stroke="#cbd5e1"
            strokeWidth={2}
            strokeOpacity={0.75}
          />
        </svg>
        <div className="mt-2 h-4" aria-hidden="true" />
      </div>
    );
  }

  const width = 520;
  const height = 160;
  const maxY = Math.max(
    ...points.flatMap((point) => visibleSeries.map((item) => item.valueFor(point))),
    1
  );
  const step = points.length > 1 ? width / (points.length - 1) : 0;
  const lines = visibleSeries.map((item) => {
    const path = points
      .map((point, index) => {
        const x = index * step;
        const value = item.valueFor(point);
        const y = height - (value / maxY) * (height - 12) - 6;
        return `${index === 0 ? "M" : "L"} ${x.toFixed(1)} ${y.toFixed(1)}`;
      })
      .join(" ");
    return { ...item, path };
  });

  return (
    <div className="rounded-md border bg-background/70 p-3">
      <svg viewBox={`0 0 ${width} ${height}`} className="h-[160px] w-full">
        {lines.map((line) => (
          <path key={line.key} d={line.path} fill="none" stroke={line.stroke} strokeWidth={2.2} />
        ))}
      </svg>
      <div className="mt-2 flex flex-wrap gap-3 text-xs text-muted-foreground">
        {lines.map((line) => (
          <div key={`${line.key}-legend`} className="inline-flex items-center gap-1">
            <span className="inline-block h-2.5 w-2.5 rounded-full" style={{ background: line.stroke }} />
            {line.key}
          </div>
        ))}
      </div>
    </div>
  );
}
