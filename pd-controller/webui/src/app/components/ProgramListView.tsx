import { Plus } from "lucide-react";

import { formatUnixMs } from "@/app/helpers";
import type { ProgramSummary } from "@/app/types";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";

type ProgramListViewProps = {
  newProgramName: string;
  onNewProgramNameChange: (value: string) => void;
  creatingProgram: boolean;
  onCreateProgram: () => void;
  programSearch: string;
  onProgramSearchChange: (value: string) => void;
  filteredPrograms: ProgramSummary[];
  onSelectProgram: (programId: string) => void;
};

export function ProgramListView({
  newProgramName,
  onNewProgramNameChange,
  creatingProgram,
  onCreateProgram,
  programSearch,
  onProgramSearchChange,
  filteredPrograms,
  onSelectProgram
}: ProgramListViewProps) {
  return (
    <div className="space-y-4">
      <div className="rounded-xl border border-slate-200/80 bg-white/80 px-4 py-4 backdrop-blur lg:px-6">
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div>
            <div className="text-xs uppercase tracking-[0.24em] text-slate-500">Workflow Registry</div>
            <div className="mt-1 text-2xl font-semibold tracking-tight">Programs</div>
            <div className="mt-1 text-sm text-muted-foreground">Store, version, and open workflows for editing.</div>
          </div>
          <div className="grid w-full gap-2 sm:max-w-[520px] sm:grid-cols-[1fr_auto]">
            <Input
              id="new-program-name"
              value={newProgramName}
              onChange={(event) => onNewProgramNameChange(event.target.value)}
              placeholder="new-program-name"
              className="h-10"
            />
            <Button onClick={onCreateProgram} disabled={creatingProgram}>
              <Plus className="mr-1 h-4 w-4" />
              {creatingProgram ? "Creating" : "Create Program"}
            </Button>
          </div>
        </div>
      </div>

      <section className="rounded-xl border border-slate-200/80 bg-white/80 p-4 backdrop-blur">
        <div className="mb-3 flex items-center justify-between gap-3">
          <div className="text-sm font-medium text-slate-700">Program Table</div>
          <Input
            value={programSearch}
            onChange={(event) => onProgramSearchChange(event.target.value)}
            placeholder="Search by name..."
            className="h-9 w-full max-w-[320px]"
          />
        </div>

        <div className="overflow-hidden rounded-lg border">
          <div className="grid grid-cols-[minmax(220px,1.4fr)_120px_110px_170px] gap-2 border-b bg-muted/40 px-3 py-2 text-[11px] uppercase tracking-wide text-muted-foreground">
            <div>Program</div>
            <div>Latest</div>
            <div>Versions</div>
            <div>Updated</div>
          </div>
          <div className="max-h-[66vh] overflow-auto">
            {filteredPrograms.map((program) => (
              <button
                key={program.program_id}
                type="button"
                onClick={() => onSelectProgram(program.program_id)}
                className="grid w-full grid-cols-[minmax(220px,1.4fr)_120px_110px_170px] items-center gap-2 border-b px-3 py-2 text-left text-sm transition hover:bg-muted/50"
              >
                <div className="truncate font-medium">{program.name}</div>
                <div>v{program.latest_version}</div>
                <div>{program.versions}</div>
                <div className="text-xs text-muted-foreground">{formatUnixMs(program.updated_unix_ms)}</div>
              </button>
            ))}
            {filteredPrograms.length === 0 ? (
              <div className="px-3 py-6 text-center text-sm text-muted-foreground">No programs match your search.</div>
            ) : null}
          </div>
        </div>
      </section>
    </div>
  );
}
