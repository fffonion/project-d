import type { DragEvent } from "react";
import { Maximize2, Minimize2, Plus } from "lucide-react";

import type { UiBlockDefinition } from "@/app/types";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

type ProgramPaletteProps = {
  floating?: boolean;
  minimized?: boolean;
  definitions: UiBlockDefinition[];
  search: string;
  onSearchChange: (value: string) => void;
  onPaletteDragStart: (event: DragEvent<HTMLDivElement>, blockId: string) => void;
  onAddNode: (blockId: string) => void;
  onToggleMinimized?: () => void;
};

export function ProgramPalette({
  floating = false,
  minimized = false,
  definitions,
  search,
  onSearchChange,
  onPaletteDragStart,
  onAddNode,
  onToggleMinimized
}: ProgramPaletteProps) {
  return (
    <Card
      className={
        floating
          ? `pointer-events-auto overflow-hidden border-slate-700 bg-white/95 text-slate-900 backdrop-blur ${
              minimized ? "w-[210px]" : "flex h-[calc(100vh-330px)] w-[320px] flex-col"
            }`
          : "h-fit"
      }
    >
      <CardHeader className={floating && minimized ? "py-2" : "pb-3"}>
        <div className="flex items-start justify-between gap-3">
          <div>
            <CardTitle>Palette</CardTitle>
            {!(floating && minimized) ? <CardDescription>Drag blocks onto the canvas</CardDescription> : null}
          </div>
          {floating ? (
            <Button
              size="sm"
              variant="ghost"
              className="h-7 w-7 px-0"
              onClick={onToggleMinimized}
              aria-label={minimized ? "Expand palette" : "Minimize palette"}
            >
              {minimized ? <Maximize2 className="h-3.5 w-3.5" /> : <Minimize2 className="h-3.5 w-3.5" />}
            </Button>
          ) : null}
        </div>
      </CardHeader>
      {floating && minimized ? null : (
        <CardContent className={floating ? "flex-1 space-y-3 overflow-auto" : "space-y-3"}>
          <div className="space-y-1">
            <Label htmlFor={floating ? "block-search" : "block-search-mobile"}>Search blocks</Label>
            <Input
              id={floating ? "block-search" : "block-search-mobile"}
              value={search}
              onChange={(event) => onSearchChange(event.target.value)}
              placeholder="if, header, rate, set..."
            />
          </div>
          {definitions.map((definition) => (
            <div
              key={floating ? definition.id : `mobile-${definition.id}`}
              className="cursor-grab rounded-md border bg-muted/40 p-3 active:cursor-grabbing"
              draggable
              onDragStart={(event) => onPaletteDragStart(event, definition.id)}
            >
              <div className="mb-1 flex items-center justify-between gap-2">
                <div className="text-sm font-semibold">{definition.title}</div>
                <Badge>{definition.category}</Badge>
              </div>
              <p className="mb-2 text-xs text-muted-foreground">{definition.description}</p>
              <Button size="sm" variant="secondary" className="w-full" onClick={() => onAddNode(definition.id)}>
                <Plus className="mr-1 h-3.5 w-3.5" />
                Add to canvas
              </Button>
            </div>
          ))}
        </CardContent>
      )}
    </Card>
  );
}
