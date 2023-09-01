import { serialize } from "https://deno.land/x/serialize_javascript/mod.ts";

const defineSQL = (filter) => {
  const D = "|&|";
  const N = "purrNULL";

  filter = filter ? filter : "";

  const where = filter.trim().length === 0 ? "" : `WHERE ${filter}`;

  let select = `SELECT * FROM (
    WITH w AS (
      SELECT
        uwi                        AS w_uwi,
        assigned_field             AS w_assigned_field,
        common_well_name           AS w_common_well_name,
        completion_date            AS w_completion_date,
        country                    AS w_country,
        county                     AS w_county,
        current_class              AS w_current_class,
        current_status             AS w_current_status,
        depth_datum                AS w_depth_datum,
        final_td                   AS w_final_td,
        ground_elev                AS w_ground_elev,
        kb_elev                    AS w_kb_elev,
        lease_name                 AS w_lease_name,
        operator                   AS w_operator,
        province_state             AS w_province_state,
        row_changed_date           AS w_row_changed_date,
        spud_date                  AS w_spud_date,
        surface_latitude           AS w_surface_latitude,
        surface_longitude          AS w_surface_longitude,
        td_form                    AS w_td_form,
        well_name                  AS w_well_name,
        well_number                AS w_well_number       
      FROM well
    ),
    c AS (
      SELECT
        uwi                        AS id_c_uwi,
        LIST(IFNULL(base_depth,                 '${N}', CAST(base_depth AS VARCHAR)),                '${D}' ORDER BY completion_obs_no)  AS c_base_depth,
        LIST(IFNULL(base_form,                  '${N}', CAST(base_form  AS VARCHAR)),                '${D}' ORDER BY completion_obs_no)  AS c_base_form,
        LIST(IFNULL(completion_date,            '${N}', CAST(completion_date  AS VARCHAR)),          '${D}' ORDER BY completion_obs_no)  AS c_completion_date,
        LIST(IFNULL(completion_form,            '${N}', CAST(completion_form AS VARCHAR)),           '${D}' ORDER BY completion_obs_no)  AS c_completion_form,
        LIST(IFNULL(completion_obs_no,          '${N}', CAST(completion_obs_no AS VARCHAR)),         '${D}' ORDER BY completion_obs_no)  AS c_completion_obs_no,
        LIST(IFNULL(completion_type,            '${N}', CAST(completion_type AS VARCHAR)),           '${D}' ORDER BY completion_obs_no)  AS c_completion_type,
        LIST(IFNULL(remark,                     '${N}', CAST(remark AS VARCHAR)),                    '${D}' ORDER BY completion_obs_no)  AS c_remark,
        LIST(IFNULL(row_changed_date,           '${N}', CAST(row_changed_date AS VARCHAR)),          '${D}' ORDER BY completion_obs_no)  AS c_row_changed_date,
        LIST(IFNULL(source,                     '${N}', CAST(source AS VARCHAR)),                    '${D}' ORDER BY completion_obs_no)  AS c_source,
        LIST(IFNULL(top_depth,                  '${N}', CAST(top_depth AS VARCHAR)),                 '${D}' ORDER BY completion_obs_no)  AS c_top_depth,
        LIST(IFNULL(top_form,                   '${N}', CAST(top_form AS VARCHAR)),                  '${D}' ORDER BY completion_obs_no)  AS c_top_form,
        LIST(IFNULL(uwi,                        '${N}', CAST(uwi AS VARCHAR)),                       '${D}' ORDER BY completion_obs_no)  AS c_uwi
      FROM well_completion
      GROUP BY uwi
    ), 
    p AS (
      SELECT 
        uwi                        AS id_p_uwi,
        LIST(IFNULL(base_depth,                 '${N}', CAST(base_depth AS VARCHAR)),                '${D}' ORDER BY perforation_obs_no) AS p_base_depth,
        LIST(IFNULL(base_form,                  '${N}', CAST(base_form AS VARCHAR)),                 '${D}' ORDER BY perforation_obs_no) AS p_base_form,
        LIST(IFNULL(cluster,                    '${N}', CAST(cluster AS VARCHAR)),                   '${D}' ORDER BY perforation_obs_no) AS p_cluster,
        LIST(IFNULL(completion_obs_no,          '${N}', CAST(completion_obs_no AS VARCHAR)),         '${D}' ORDER BY perforation_obs_no) AS p_completion_obs_no,
        LIST(IFNULL(completion_source,          '${N}', CAST(completion_source AS VARCHAR)),         '${D}' ORDER BY perforation_obs_no) AS p_completion_source,
        LIST(IFNULL(current_status,             '${N}', CAST(current_status AS VARCHAR)),            '${D}' ORDER BY perforation_obs_no) AS p_current_status,
        LIST(IFNULL(gx_base_form_alias,         '${N}', CAST(gx_base_form_alias AS VARCHAR)),        '${D}' ORDER BY perforation_obs_no) AS p_gx_base_form_alias,
        LIST(IFNULL(gx_top_form_alias,          '${N}', CAST(gx_top_form_alias AS VARCHAR)),         '${D}' ORDER BY perforation_obs_no) AS p_gx_top_form_alias,
        LIST(IFNULL(perforation_angle,          '${N}', CAST(perforation_angle AS VARCHAR)),         '${D}' ORDER BY perforation_obs_no) AS p_perforation_angle,
        LIST(IFNULL(perforation_count,          '${N}', CAST(perforation_count AS VARCHAR)),         '${D}' ORDER BY perforation_obs_no) AS p_perforation_count,
        LIST(IFNULL(perforation_date,           '${N}', CAST(perforation_date AS VARCHAR)),          '${D}' ORDER BY perforation_obs_no) AS p_perforation_date,
        LIST(IFNULL(perforation_density,        '${N}', CAST(perforation_density AS VARCHAR)),       '${D}' ORDER BY perforation_obs_no) AS p_perforation_density,
        LIST(IFNULL(perforation_diameter,       '${N}', CAST(perforation_diameter AS VARCHAR)),      '${D}' ORDER BY perforation_obs_no) AS p_perforation_diameter,
        LIST(IFNULL(perforation_diameter_ouom,  '${N}', CAST(perforation_diameter_ouom AS VARCHAR)), '${D}' ORDER BY perforation_obs_no) AS p_perforation_diameter_ouom,
        LIST(IFNULL(perforation_obs_no,         '${N}', CAST(perforation_obs_no AS VARCHAR)),        '${D}' ORDER BY perforation_obs_no) AS p_perforation_obs_no,
        LIST(IFNULL(perforation_per_uom,        '${N}', CAST(perforation_per_uom AS VARCHAR)),       '${D}' ORDER BY perforation_obs_no) AS p_perforation_per_uom,
        LIST(IFNULL(perforation_phase,          '${N}', CAST(perforation_phase AS VARCHAR)),         '${D}' ORDER BY perforation_obs_no) AS p_perforation_phase,
        LIST(IFNULL(perforation_type,           '${N}', CAST(perforation_type AS VARCHAR)),          '${D}' ORDER BY perforation_obs_no) AS p_perforation_type,
        LIST(IFNULL(remark,                     '${N}', CAST(remark AS VARCHAR)),                    '${D}' ORDER BY perforation_obs_no) AS p_remark,
        LIST(IFNULL(row_changed_date,           '${N}', CAST(row_changed_date AS VARCHAR)),          '${D}' ORDER BY perforation_obs_no) AS p_row_changed_date,
        LIST(IFNULL(source,                     '${N}', CAST(source AS VARCHAR)),                    '${D}' ORDER BY perforation_obs_no) AS p_source,
        LIST(IFNULL(stage,                      '${N}', CAST(stage AS VARCHAR)),                     '${D}' ORDER BY perforation_obs_no) AS p_stage,
        LIST(IFNULL(top_depth,                  '${N}', CAST(top_depth AS VARCHAR)),                 '${D}' ORDER BY perforation_obs_no) AS p_top_depth,
        LIST(IFNULL(top_form,                   '${N}', CAST(top_form AS VARCHAR)),                  '${D}' ORDER BY perforation_obs_no) AS p_top_form,
        LIST(IFNULL(uwi,                        '${N}', CAST(uwi AS VARCHAR)),                       '${D}' ORDER BY perforation_obs_no) AS p_uwi
      FROM well_perforation
      GROUP BY uwi
    )
    SELECT
      w.*,
      c.*,
      p.*
    FROM w
    JOIN c ON w.w_uwi = c.id_c_uwi
    JOIN p ON w.w_uwi = p.id_p_uwi
    ) x`;

  const order = `ORDER BY w_uwi`;

  const count = `SELECT COUNT(*) AS count FROM ( ${select} ) c ${where}`;

  //const fast_count = `SELECT COUNT(DISTINCT wellid) AS count FROM gx_well_curve`;

  return {
    select: select,
    count: count,
    order: order,
    where: where,
  };
};

const xformer = (args) => {
  let { func, key, arg, obj } = args;

  const D = "|&|";
  const N = "purrNULL";

  if (obj[key] == null) {
    return null;
  }

  switch (func) {
    case "delimited_array_with_nulls":
      return (() => {
        try {
          return obj[key].split(D).map((v) => (v === N ? null : v));
        } catch (error) {
          console.log("ERROR", error);
          return;
        }
      })();
    default:
      console.warn("UNEXPECTED TRANSFORM FUNC: " + func);
      return obj[key];
  }
};

const xforms = {
  c_base_depth: "delimited_array_with_nulls",
  c_base_form: "delimited_array_with_nulls",
  c_completion_date: "delimited_array_with_nulls",
  c_completion_form: "delimited_array_with_nulls",
  c_completion_obs_no: "delimited_array_with_nulls",
  c_completion_type: "delimited_array_with_nulls",
  c_remark: "delimited_array_with_nulls",
  c_row_changed_date: "delimited_array_with_nulls",
  c_source: "delimited_array_with_nulls",
  c_top_depth: "delimited_array_with_nulls",
  c_top_form: "delimited_array_with_nulls",
  c_uwi: "delimited_array_with_nulls",
  p_base_depth: "delimited_array_with_nulls",
  p_base_form: "delimited_array_with_nulls",
  p_cluster: "delimited_array_with_nulls",
  p_completion_obs_no: "delimited_array_with_nulls",
  p_completion_source: "delimited_array_with_nulls",
  p_current_status: "delimited_array_with_nulls",
  p_gx_base_form_alias: "delimited_array_with_nulls",
  p_gx_top_form_alias: "delimited_array_with_nulls",
  p_gx_top_form_alias: "delimited_array_with_nulls",
  p_perforation_angle: "delimited_array_with_nulls",
  p_perforation_count: "delimited_array_with_nulls",
  p_perforation_date: "delimited_array_with_nulls",
  p_perforation_density: "delimited_array_with_nulls",
  p_perforation_diameter: "delimited_array_with_nulls",
  p_perforation_diameter_ouom: "delimited_array_with_nulls",
  p_perforation_obs_no: "delimited_array_with_nulls",
  p_perforation_per_uom: "delimited_array_with_nulls",
  p_perforation_phase: "delimited_array_with_nulls",
  p_perforation_type: "delimited_array_with_nulls",
  p_remark: "delimited_array_with_nulls",
  p_row_changed_date: "delimited_array_with_nulls",
  p_source: "delimited_array_with_nulls",
  p_stage: "delimited_array_with_nulls",
  p_top_depth: "delimited_array_with_nulls",
  p_top_form: "delimited_array_with_nulls",
  p_uwi: "delimited_array_with_nulls",
};

const prefixes = {
  w_: "well",
  c_: "well_completion",
  p_: "well_perforation",
};

const global_id_keys = ["w_uwi"];

const well_id_keys = ["w_uwi"];

const pg_cols = ["id", "repo_id", "well_id", "geo_type", "tag", "doc"];

const default_chunk = 1000;

///////////////////////////////////////////////////////////////////////////

export const getAssetDNA = (filter) => {
  return {
    default_chunk: default_chunk,
    global_id_keys: global_id_keys,
    pg_cols: pg_cols,
    prefixes: prefixes,
    serialized_xformer: serialize(xformer),
    sql: defineSQL(filter),
    well_id_keys: well_id_keys,
    xforms: xforms,
  };
};
