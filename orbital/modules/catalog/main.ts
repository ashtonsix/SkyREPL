#!/usr/bin/env bun
// orbital/modules/catalog/main.ts - Catalog module entry point
//
// Standalone executable: reads stdin JSON, writes stdout JSON, exit code 0/non-zero.
// Module interface per orbital design: stdin → stdout, exit code.

import type { CatalogRequest, CatalogResponse } from "./types";
import { resolveSpec, listInstances } from "./resolve";
import { ALL_REGIONS } from "./data/regions";
import { ALL_GPUS } from "./data/gpus";
import { ALL_INSTANCES } from "./data/instances";

async function main(): Promise<void> {
  let input: string;
  try {
    input = await Bun.stdin.text();
  } catch {
    const resp: CatalogResponse = { ok: false, action: "unknown", error: "Failed to read stdin" };
    process.stdout.write(JSON.stringify(resp));
    process.exit(1);
  }

  let request: CatalogRequest;
  try {
    request = JSON.parse(input);
  } catch {
    const resp: CatalogResponse = { ok: false, action: "unknown", error: "Invalid JSON input" };
    process.stdout.write(JSON.stringify(resp));
    process.exit(1);
  }

  let response: CatalogResponse;

  switch (request.action) {
    case "resolve-spec": {
      const results = resolveSpec(request);
      response = { ok: true, action: "resolve-spec", results };
      break;
    }

    case "list-instances": {
      const instances = listInstances(request);
      response = { ok: true, action: "list-instances", instances };
      break;
    }

    case "list-regions": {
      let regions = ALL_REGIONS;
      if (request.provider) {
        regions = regions.filter(r => r.provider === request.provider);
      }
      response = { ok: true, action: "list-regions", regions };
      break;
    }

    case "list-gpus": {
      let gpus = ALL_GPUS;
      if (request.gpuModel) {
        gpus = gpus.filter(g => g.model.toLowerCase() === request.gpuModel!.toLowerCase());
      }
      response = { ok: true, action: "list-gpus", gpus };
      break;
    }

    case "get-pricing": {
      let instances = ALL_INSTANCES;
      if (request.provider) {
        instances = instances.filter(i => i.provider === request.provider);
      }
      if (request.spec) {
        instances = instances.filter(i => i.spec === request.spec);
      }
      const pricing = instances.map(i => ({
        provider: i.provider,
        spec: i.spec,
        onDemandHourly: i.onDemandHourly,
        spotHourly: i.spotHourly,
      }));
      response = { ok: true, action: "get-pricing", pricing };
      break;
    }

    default:
      response = { ok: false, action: request.action, error: `Unknown action: ${request.action}` };
      process.stdout.write(JSON.stringify(response));
      process.exit(1);
  }

  process.stdout.write(JSON.stringify(response));
  process.exit(0);
}

main();
