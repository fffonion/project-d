import { Maximize2, Minimize2 } from "lucide-react";

import { HighlightedCode } from "@/app/components/HighlightedCode";
import type { SourceFlavor, UiSourceBundle } from "@/app/types";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";

type GeneratedCodePanelProps = {
  floating?: boolean;
  minimized?: boolean;
  rendering: boolean;
  activeFlavor: SourceFlavor;
  source: UiSourceBundle;
  onFlavorChange: (value: SourceFlavor) => void;
  onToggleMinimized?: () => void;
  onEdit?: () => void;
  readOnly?: boolean;
  onCodeChange?: (flavor: SourceFlavor, value: string) => void;
  editorHeight?: string;
  description?: string;
  title?: string;
  showHeader?: boolean;
};

export function GeneratedCodePanel({
  floating = false,
  minimized = false,
  rendering,
  activeFlavor,
  source,
  onFlavorChange,
  onToggleMinimized,
  onEdit,
  readOnly = true,
  onCodeChange,
  editorHeight = "520px",
  description,
  title = "Generated Code",
  showHeader = true
}: GeneratedCodePanelProps) {
  const panelDescription =
    description ?? `render=${rendering ? "updating" : "synced"} deploy_flavor=${activeFlavor}`;

  return (
    <Card
      className={
        floating
          ? `pointer-events-auto overflow-hidden border-slate-700 bg-white/95 backdrop-blur ${
              minimized ? "w-[220px]" : "flex h-[calc(100vh-330px)] w-[440px] flex-col"
            }`
          : "border-slate-200/80 bg-white/90 shadow-xl backdrop-blur"
      }
    >
      {showHeader ? (
        <CardHeader className={floating && minimized ? "py-2" : "pb-3"}>
          <div className="flex items-start justify-between gap-3">
            <div>
              <CardTitle>{title}</CardTitle>
              {!(floating && minimized) ? <CardDescription>{panelDescription}</CardDescription> : null}
            </div>
            <div className="flex items-center gap-1">
              {onEdit && !(floating && minimized) ? (
                <Button size="sm" variant="outline" className="h-7 px-2 text-xs" onClick={onEdit}>
                  Edit
                </Button>
              ) : null}
              {floating ? (
                <Button
                  size="sm"
                  variant="ghost"
                  className="h-7 w-7 px-0"
                  onClick={onToggleMinimized}
                  aria-label={minimized ? "Expand generated code panel" : "Minimize generated code panel"}
                >
                  {minimized ? <Maximize2 className="h-3.5 w-3.5" /> : <Minimize2 className="h-3.5 w-3.5" />}
                </Button>
              ) : null}
            </div>
          </div>
        </CardHeader>
      ) : null}
      {floating && minimized ? null : (
        <CardContent className={floating ? "flex-1 overflow-auto" : ""}>
          <Tabs
            value={activeFlavor}
            onValueChange={(value) => onFlavorChange(value as SourceFlavor)}
            className={floating ? "flex h-full flex-col" : ""}
          >
            <TabsList className="grid w-full grid-cols-4">
              <TabsTrigger value="rustscript">RustScript</TabsTrigger>
              <TabsTrigger value="javascript">JavaScript</TabsTrigger>
              <TabsTrigger value="lua">Lua</TabsTrigger>
              <TabsTrigger value="scheme">Scheme</TabsTrigger>
            </TabsList>
            <TabsContent value="rustscript" className={floating ? "flex-1 overflow-auto" : ""}>
              <HighlightedCode
                flavor="rustscript"
                source={source}
                readOnly={readOnly}
                height={editorHeight}
                onChange={(value) => onCodeChange?.("rustscript", value)}
              />
            </TabsContent>
            <TabsContent value="javascript" className={floating ? "flex-1 overflow-auto" : ""}>
              <HighlightedCode
                flavor="javascript"
                source={source}
                readOnly={readOnly}
                height={editorHeight}
                onChange={(value) => onCodeChange?.("javascript", value)}
              />
            </TabsContent>
            <TabsContent value="lua" className={floating ? "flex-1 overflow-auto" : ""}>
              <HighlightedCode
                flavor="lua"
                source={source}
                readOnly={readOnly}
                height={editorHeight}
                onChange={(value) => onCodeChange?.("lua", value)}
              />
            </TabsContent>
            <TabsContent value="scheme" className={floating ? "flex-1 overflow-auto" : ""}>
              <HighlightedCode
                flavor="scheme"
                source={source}
                readOnly={readOnly}
                height={editorHeight}
                onChange={(value) => onCodeChange?.("scheme", value)}
              />
            </TabsContent>
          </Tabs>
        </CardContent>
      )}
    </Card>
  );
}
