import { serialize } from "https://deno.land/x/serialize_javascript/mod.ts";

// ignored: fault.fault_id, r_source.source

const defineSQL = (filter, recency) => {
  const D = "|&|";
  const N = "purrNULL";

  filter = filter ? filter : "";
  const whereClause = ["WHERE 1=1"];
  if (filter.trim().length > 0) {
    whereClause.push(filter);
  }
  const where = whereClause.join(" AND ");

  let whereRecent = "";
  if (recency > 0) {
    whereRecent = `WHERE row_changed_date >= DATEADD(DAY, -${recency}, CURRENT DATE)`;
  }

  let select = `SELECT * FROM (
    WITH w AS (
      SELECT
        uwi                       AS w_uwi
      FROM well
    ),
    f AS (
      SELECT
        uwi                       AS id_f_uwi,
        MAX(row_changed_date)     AS max_row_changed_date,
        LIST(IFNULL(dominant_lithology,      '${N}', CAST(dominant_lithology AS VARCHAR)),      '${D}' ORDER BY form_obs_no) AS f_dominant_lithology,
        LIST(IFNULL(fault_name,              '${N}', CAST(fault_name AS VARCHAR)),              '${D}' ORDER BY form_obs_no) AS f_fault_name,
        LIST(IFNULL(faulted_ind,             '${N}', CAST(faulted_ind AS VARCHAR)),             '${D}' ORDER BY form_obs_no) AS f_faulted_ind,
        LIST(IFNULL(form_depth,              '${N}', CAST(form_depth AS VARCHAR)),              '${D}' ORDER BY form_obs_no) AS f_form_depth,
        LIST(IFNULL(form_id,                 '${N}', CAST(form_id AS VARCHAR)),                 '${D}' ORDER BY form_obs_no) AS f_form_id,
        LIST(IFNULL(form_obs_no,             '${N}', CAST(form_obs_no AS VARCHAR)),             '${D}' ORDER BY form_obs_no) AS f_form_obs_no,
        LIST(IFNULL(form_tvd,                '${N}', CAST(form_tvd AS VARCHAR)),                '${D}' ORDER BY form_obs_no) AS f_form_tvd,
        LIST(IFNULL(gap_thickness,           '${N}', CAST(gap_thickness AS VARCHAR)),           '${D}' ORDER BY form_obs_no) AS f_gap_thickness,
        LIST(IFNULL(gx_base_subsea,          '${N}', CAST(gx_base_subsea AS VARCHAR)),          '${D}' ORDER BY form_obs_no) AS f_gx_base_subsea,
        LIST(IFNULL(gx_dip,                  '${N}', CAST(gx_dip AS VARCHAR)),                  '${D}' ORDER BY form_obs_no) AS f_gx_dip,
        LIST(IFNULL(gx_dip_azimuth,          '${N}', CAST(gx_dip_azimuth AS VARCHAR)),          '${D}' ORDER BY form_obs_no) AS f_gx_dip_azimuth,
        LIST(IFNULL(gx_eroded_ind,           '${N}', CAST(gx_eroded_ind AS VARCHAR)),           '${D}' ORDER BY form_obs_no) AS f_gx_eroded_ind,
        LIST(IFNULL(gx_exten_id,             '${N}', CAST(gx_exten_id AS VARCHAR)),             '${D}' ORDER BY form_obs_no) AS f_gx_exten_id,
        LIST(IFNULL(gx_form_base_depth,      '${N}', CAST(gx_form_base_depth AS VARCHAR)),      '${D}' ORDER BY form_obs_no) AS f_gx_form_base_depth,
        LIST(IFNULL(gx_form_base_tvd,        '${N}', CAST(gx_form_base_tvd AS VARCHAR)),        '${D}' ORDER BY form_obs_no) AS f_gx_form_base_tvd,
        LIST(IFNULL(gx_form_depth_datum,     '${N}', CAST(gx_form_depth_datum AS VARCHAR)),     '${D}' ORDER BY form_obs_no) AS f_gx_form_depth_datum,
        LIST(IFNULL(gx_form_id_alias,        '${N}', CAST(gx_form_id_alias AS VARCHAR)),        '${D}' ORDER BY form_obs_no) AS f_gx_form_id_alias,
        LIST(IFNULL(gx_form_top_depth,       '${N}', CAST(gx_form_top_depth AS VARCHAR)),       '${D}' ORDER BY form_obs_no) AS f_gx_form_top_depth,
        LIST(IFNULL(gx_form_top_tvd,         '${N}', CAST(gx_form_top_tvd AS VARCHAR)),         '${D}' ORDER BY form_obs_no) AS f_gx_form_top_tvd,
        LIST(IFNULL(gx_form_x_coordinate,    '${N}', CAST(gx_form_x_coordinate AS VARCHAR)),    '${D}' ORDER BY form_obs_no) AS f_gx_form_x_coordinate,
        LIST(IFNULL(gx_form_y_coordinate,    '${N}', CAST(gx_form_y_coordinate AS VARCHAR)),    '${D}' ORDER BY form_obs_no) AS f_gx_form_y_coordinate,
        LIST(IFNULL(gx_gross_thickness,      '${N}', CAST(gx_gross_thickness AS VARCHAR)),      '${D}' ORDER BY form_obs_no) AS f_gx_gross_thickness,
        LIST(IFNULL(gx_internal_no,          '${N}', CAST(gx_internal_no AS VARCHAR)),          '${D}' ORDER BY form_obs_no) AS f_gx_internal_no,
        LIST(IFNULL(gx_net_thickness,        '${N}', CAST(gx_net_thickness AS VARCHAR)),        '${D}' ORDER BY form_obs_no) AS f_gx_net_thickness,
        LIST(IFNULL(gx_porosity,             '${N}', CAST(gx_porosity AS VARCHAR)),             '${D}' ORDER BY form_obs_no) AS f_gx_porosity,
        LIST(IFNULL(gx_show,                 '${N}', CAST(gx_show AS VARCHAR)),                 '${D}' ORDER BY form_obs_no) AS f_gx_show,
        LIST(IFNULL(gx_strat_column,         '${N}', CAST(gx_strat_column AS VARCHAR)),         '${D}' ORDER BY form_obs_no) AS f_gx_strat_column,
        LIST(IFNULL(gx_top_subsea,           '${N}', CAST(gx_top_subsea AS VARCHAR)),           '${D}' ORDER BY form_obs_no) AS f_gx_top_subsea,
        LIST(IFNULL(gx_true_strat_thickness, '${N}', CAST(gx_true_strat_thickness AS VARCHAR)), '${D}' ORDER BY form_obs_no) AS f_gx_true_strat_thickness,
        LIST(IFNULL(gx_true_vert_thickness,  '${N}', CAST(gx_true_vert_thickness AS VARCHAR)),  '${D}' ORDER BY form_obs_no) AS f_gx_true_vert_thickness,
        LIST(IFNULL(gx_user1,                '${N}', CAST(gx_user1 AS VARCHAR)),                '${D}' ORDER BY form_obs_no) AS f_gx_user1,
        LIST(IFNULL(gx_user2,                '${N}', CAST(gx_user2 AS VARCHAR)),                '${D}' ORDER BY form_obs_no) AS f_gx_user2,
        LIST(IFNULL(gx_user3,                '${N}', CAST(gx_user3 AS VARCHAR)),                '${D}' ORDER BY form_obs_no) AS f_gx_user3,
        LIST(IFNULL(gx_user4,                '${N}', CAST(gx_user4 AS VARCHAR)),                '${D}' ORDER BY form_obs_no) AS f_gx_user4,
        LIST(IFNULL(gx_user5,                '${N}', CAST(gx_user5 AS VARCHAR)),                '${D}' ORDER BY form_obs_no) AS f_gx_user5,
        LIST(IFNULL(gx_user6,                '${N}', CAST(gx_user6 AS VARCHAR)),                '${D}' ORDER BY form_obs_no) AS f_gx_user6,
        LIST(IFNULL(gx_vendor_no,            '${N}', CAST(gx_vendor_no AS VARCHAR)),            '${D}' ORDER BY form_obs_no) AS f_gx_vendor_no,
        LIST(IFNULL(gx_wellbore_angle,       '${N}', CAST(gx_wellbore_angle AS VARCHAR)),       '${D}' ORDER BY form_obs_no) AS f_gx_wellbore_angle,
        LIST(IFNULL(gx_wellbore_azimuth,     '${N}', CAST(gx_wellbore_azimuth AS VARCHAR)),     '${D}' ORDER BY form_obs_no) AS f_gx_wellbore_azimuth,
        LIST(IFNULL(percent_thickness,       '${N}', CAST(percent_thickness AS VARCHAR)),       '${D}' ORDER BY form_obs_no) AS f_percent_thickness,
        LIST(IFNULL(pick_location,           '${N}', CAST(pick_location AS VARCHAR)),           '${D}' ORDER BY form_obs_no) AS f_pick_location,
        LIST(IFNULL(pick_qualifier,          '${N}', CAST(pick_qualifier AS VARCHAR)),          '${D}' ORDER BY form_obs_no) AS f_pick_qualifier,
        LIST(IFNULL(pick_quality,            '${N}', CAST(pick_quality AS VARCHAR)),            '${D}' ORDER BY form_obs_no) AS f_pick_quality,
        LIST(IFNULL(pick_type,               '${N}', CAST(pick_type AS VARCHAR)),               '${D}' ORDER BY form_obs_no) AS f_pick_type,
        LIST(IFNULL(public,                  '${N}', CAST(public AS VARCHAR)),                  '${D}' ORDER BY form_obs_no) AS f_public,
        LIST(IFNULL(remark,                  '${N}', CAST(remark AS VARCHAR)),                  '${D}' ORDER BY form_obs_no) AS f_remark,
        LIST(IFNULL(row_changed_date,        '${N}', CAST(row_changed_date AS VARCHAR)),        '${D}' ORDER BY form_obs_no) AS f_row_changed_date,
        LIST(IFNULL(source,                  '${N}', CAST(source AS VARCHAR)),                  '${D}' ORDER BY form_obs_no) AS f_source,
        LIST(IFNULL(unc_fault_obs_no,        '${N}', CAST(unc_fault_obs_no AS VARCHAR)),        '${D}' ORDER BY form_obs_no) AS f_unc_fault_obs_no,
        LIST(IFNULL(unc_fault_source,        '${N}', CAST(unc_fault_source AS VARCHAR)),        '${D}' ORDER BY form_obs_no) AS f_unc_fault_source,
        LIST(IFNULL(unconformity_name,       '${N}', CAST(unconformity_name AS VARCHAR)),       '${D}' ORDER BY form_obs_no) AS f_unconformity_name,
        LIST(IFNULL(uwi,                     '${N}', CAST(uwi AS VARCHAR)),                     '${D}' ORDER BY form_obs_no) AS f_uwi
      FROM well_formation
      ${whereRecent}
      GROUP BY uwi
    )
    SELECT 
      w.*,
      f.*
    FROM w
    JOIN f ON
      f.id_f_uwi = w.w_uwi
    ) x`;

  //const order = `ORDER BY w_uwi, f_form_id, f_form_obs_no, f_source`;
  const order = `ORDER BY w_uwi`;

  const count = `SELECT COUNT(*) AS count FROM ( ${select} ) c ${where}`;

  return {
    select: select,
    count: count,
    order: order,
    where: where,
  };
};

const xformer = (args) => {
  const D = "|&|";
  const N = "purrNULL";

  let { func, key, typ, arg, obj } = args;

  const ensureType = (type: string, val: any) => {
    if (val == null) {
      return null;
    } else if (type === "object") {
      console.log("UNEXPECTED OBJECT TYPE! (needs xformer)", type);
      console.log(val);
      return null;
    } else if (type === "string") {
      return val.replace(/[\u0000-\u001F\u007F-\u009F]/g, "");
    } else if (type === "number") {
      if (val.toString().replace(/\s/g, "") === "") {
        return null;
      }
      let n = Number(val);
      return isNaN(n) ? null : n;
    } else if (type === "date") {
      try {
        return new Date(val).toISOString();
      } catch (error) {
        return null;
      }
    } else {
      console.log("ENSURE TYPE SOMETHING ELSE (xformer)", type);
      return "XFORM ME";
    }
  };

  if (obj[key] == null) {
    return null;
  }

  switch (func) {
    case "delimited_array_with_nulls":
      return (() => {
        try {
          return obj[key]
            .split(D)
            .map((v) => (v === N ? null : ensureType(typ, v)));
        } catch (error) {
          console.log("ERROR", error);
          return;
        }
      })();
    default:
      return ensureType(typ, obj[key]);
  }
};

const xforms = {
  // WELL

  w_uwi: {
    ts_type: "string",
  },

  // WELL_FORMATION

  id_f_uwi: {
    ts_type: "string",
  },
  max_row_changed_date: {
    ts_type: "date",
  },

  f_dominant_lithology: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_fault_name: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_faulted_ind: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_form_depth: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_form_id: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_form_obs_no: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_form_tvd: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gap_thickness: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_base_subsea: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_dip: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_gx_dip_azimuth: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_eroded_ind: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_exten_id: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_gx_form_base_depth: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_form_base_tvd: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_form_depth_datum: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_form_id_alias: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_gx_form_top_depth: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_form_top_tvd: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_form_x_coordinate: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_form_y_coordinate: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_gross_thickness: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_internal_no: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_net_thickness: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_porosity: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_show: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_gx_strat_column: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_gx_top_subsea: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_true_strat_thickness: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_true_vert_thickness: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_user1: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_gx_user2: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_gx_user3: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_gx_user4: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_user5: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_user6: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_vendor_no: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_wellbore_angle: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_wellbore_azimuth: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_percent_thickness: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_pick_location: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_pick_qualifier: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_pick_quality: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_pick_type: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_public: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_remark: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_row_changed_date: {
    ts_type: "date",
    xform: "delimited_array_with_nulls",
  },
  f_source: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_unc_fault_obs_no: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_unc_fault_source: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_unconformity_name: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_uwi: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
};

const prefixes = {
  w_: "well",
  f_: "well_formation",
};

//const asset_id_keys = ["w_uwi", "f_form_id", "f_form_obs_no", "f_source"];
const asset_id_keys = ["w_uwi"];

const well_id_keys = ["w_uwi"];

const default_chunk = 100; // 200

///////////////////////////////////////////////////////////////////////////////

export const getAssetDNA = (filter, recency) => {
  return {
    asset_id_keys: asset_id_keys,
    default_chunk: default_chunk,
    prefixes: prefixes,
    serialized_xformer: serialize(xformer),
    sql: defineSQL(filter, recency),
    well_id_keys: well_id_keys,
    xforms: xforms,
    notes: [],
  };
};
