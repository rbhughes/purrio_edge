// Follow this setup guide to integrate the Deno language server with your editor:
// https://deno.land/manual/getting_started/setup_your_environment
// This enables autocomplete, go to definition, etc.

import { corsHeaders } from "../_shared/cors.ts";
// import { serve } from "https://deno.land/std@0.168.0/http/server.ts";

import { getAssetDNA as completion_dna } from "./completion.ts";
import { getAssetDNA as core_dna } from "./core.ts";
import { getAssetDNA as dst_dna } from "./dst.ts";
import { getAssetDNA as formation_dna } from "./formation.ts";
import { getAssetDNA as ip_dna } from "./ip.ts";
import { getAssetDNA as perforation_dna } from "./perforation.ts";
import { getAssetDNA as production_dna } from "./production.ts";
import { getAssetDNA as raster_log_dna } from "./raster_log.ts";
import { getAssetDNA as survey_dna } from "./survey.ts";
import { getAssetDNA as vector_log_dna } from "./vector_log.ts";
import { getAssetDNA as well_dna } from "./well.ts";
import { getAssetDNA as zone_dna } from "./zone.ts";

const vault = {
  completion: completion_dna,
  core: core_dna,
  dst: dst_dna,
  formation: formation_dna,
  ip: ip_dna,
  perforation: perforation_dna,
  production: production_dna,
  raster_log: raster_log_dna,
  survey: survey_dna,
  vector_log: vector_log_dna,
  well: well_dna,
  zone: zone_dna,
};

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }
  let { asset, filter, recency } = await req.json();

  const getDNA = vault[asset];

  if (!getDNA) {
    const error = `Unknown GeoGraphix asset: ${asset}`;
    console.error(error);
    return new Response(JSON.stringify(error), {
      headers: { ...corsHeaders, "Content-Type": "application/json" },
      status: 400,
    });
  }

  const data = getDNA(filter, recency);

  return new Response(JSON.stringify(data), {
    headers: { ...corsHeaders, "Content-Type": "application/json" },
    status: 200,
  });
});

// To invoke:
// curl -i --location --request POST 'http://localhost:54321/functions/v1/' \
//   --header 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZS1kZW1vIiwicm9sZSI6ImFub24iLCJleHAiOjE5ODM4MTI5OTZ9.CRXP1A7WOeoJeXxjNni43kdQwgnWNReilDMblYTn_I0' \
//   --header 'Content-Type: application/json' \
//   --data '{"name":"Functions"}'
