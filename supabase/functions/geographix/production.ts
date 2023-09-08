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
    m AS (
      SELECT
        uwi AS m_uwi,
        pden_source AS r_source,
        LIST(IFNULL(uwi,                   '${N}', uwi),                                    '${D}') AS a_m_uwi,
        LIST(IFNULL(activity_type,         '${N}', activity_type),                          '${D}') AS m_activity_type,
        LIST(IFNULL(pden_date,             '${N}', CAST(pden_date AS VARCHAR)),             '${D}') AS m_pden_date,
        LIST(IFNULL(pden_source,           '${N}', pden_source),                            '${D}') AS m_pden_source,
        LIST(IFNULL(pden_type,             '${N}', pden_type),                              '${D}') AS m_pden_type,
        LIST(IFNULL(volume_method,         '${N}', volume_method),                          '${D}') AS m_volume_method,
        LIST(IFNULL(zone_id,               '${N}', zone_id),                                '${D}') AS m_zone_id,
        LIST(IFNULL(boe_cum_volume,        '${N}', CAST(boe_cum_volume AS VARCHAR)),        '${D}') AS m_boe_cum_volume,
        LIST(IFNULL(boe_volume,            '${N}', CAST(boe_volume AS VARCHAR)),            '${D}') AS m_boe_volume,
        LIST(IFNULL(boe_ytd_volume,        '${N}', CAST(boe_ytd_volume AS VARCHAR)),        '${D}') AS m_boe_ytd_volume,
        LIST(IFNULL(co2_cum_volume,        '${N}', CAST(co2_cum_volume AS VARCHAR)),        '${D}') AS m_co2_cum_volume,
        LIST(IFNULL(co2_volume,            '${N}', CAST(co2_volume AS VARCHAR)),            '${D}') AS m_co2_volume,
        LIST(IFNULL(co2_ytd_volume,        '${N}', CAST(co2_ytd_volume AS VARCHAR)),        '${D}') AS m_co2_ytd_volume,
        LIST(IFNULL(cum_flag,              '${N}', CAST(cum_flag AS VARCHAR)),              '${D}') AS m_cum_flag,
        LIST(IFNULL(gas_cum_volume,        '${N}', CAST(gas_cum_volume AS VARCHAR)),        '${D}') AS m_gas_cum_volume,
        LIST(IFNULL(gas_quality,           '${N}', CAST(gas_quality AS VARCHAR)),           '${D}') AS m_gas_quality,
        LIST(IFNULL(gas_quality_ouom,      '${N}', gas_quality_ouom),                       '${D}') AS m_gas_quality_ouom,
        LIST(IFNULL(gas_volume,            '${N}', CAST(gas_volume AS VARCHAR)),            '${D}') AS m_gas_volume,
        LIST(IFNULL(gas_ytd_volume,        '${N}', CAST(gas_ytd_volume AS VARCHAR)),        '${D}') AS m_gas_ytd_volume,
        LIST(IFNULL(gx_percent_allocation, '${N}', CAST(gx_percent_allocation AS VARCHAR)), '${D}') AS m_gx_percent_allocation,
        LIST(IFNULL(injection_cum_volume,  '${N}', CAST(injection_cum_volume AS VARCHAR)),  '${D}') AS m_injection_cum_volume,
        LIST(IFNULL(injection_pressure,    '${N}', CAST(injection_pressure AS VARCHAR)),    '${D}') AS m_injection_pressure,
        LIST(IFNULL(injection_product,     '${N}', CAST(injection_product AS VARCHAR)),     '${D}') AS m_injection_product,
        LIST(IFNULL(injection_volume,      '${N}', CAST(injection_volume AS VARCHAR)),      '${D}') AS m_injection_volume,
        LIST(IFNULL(injection_volume_uom,  '${N}', injection_volume_uom),                   '${D}') AS m_injection_volume_uom,
        LIST(IFNULL(injection_ytd_volume,  '${N}', CAST(injection_ytd_volume AS VARCHAR)),  '${D}') AS m_injection_ytd_volume,
        LIST(IFNULL(ngl_cum_volume,        '${N}', CAST(ngl_cum_volume AS VARCHAR)),        '${D}') AS m_ngl_cum_volume,
        LIST(IFNULL(ngl_volume,            '${N}', CAST(ngl_volume AS VARCHAR)),            '${D}') AS m_ngl_volume,
        LIST(IFNULL(ngl_volume_uom,        '${N}', ngl_volume_uom),                         '${D}') AS m_ngl_volume_uom,
        LIST(IFNULL(ngl_ytd_volume,        '${N}', CAST(ngl_ytd_volume AS VARCHAR)),        '${D}') AS m_ngl_ytd_volume,
        LIST(IFNULL(nitrogen_cum_volume,   '${N}', CAST(nitrogen_cum_volume AS VARCHAR)),   '${D}') AS m_nitrogen_cum_volume,
        LIST(IFNULL(nitrogen_volume,       '${N}', CAST(nitrogen_volume AS VARCHAR)),       '${D}') AS m_nitrogen_volume,
        LIST(IFNULL(nitrogen_ytd_volume,   '${N}', CAST(nitrogen_ytd_volume AS VARCHAR)),   '${D}') AS m_nitrogen_ytd_volume,
        LIST(IFNULL(no_of_gas_wells,       '${N}', CAST(no_of_gas_wells AS VARCHAR)),       '${D}') AS m_no_of_gas_wells,
        LIST(IFNULL(no_of_inj_wells,       '${N}', CAST(no_of_inj_wells AS VARCHAR)),       '${D}') AS m_no_of_inj_wells,
        LIST(IFNULL(no_of_oil_wells,       '${N}', CAST(no_of_oil_wells AS VARCHAR)),       '${D}') AS m_no_of_oil_wells,
        LIST(IFNULL(oil_cum_volume,        '${N}', CAST(oil_cum_volume AS VARCHAR)),        '${D}') AS m_oil_cum_volume,
        LIST(IFNULL(oil_quality,           '${N}', CAST(oil_quality AS VARCHAR)),           '${D}') AS m_oil_quality,
        LIST(IFNULL(oil_quality_ouom,      '${N}', oil_quality_ouom),                       '${D}') AS m_oil_quality_ouom,
        LIST(IFNULL(oil_volume,            '${N}', CAST(oil_volume AS VARCHAR)),            '${D}') AS m_oil_volume,
        LIST(IFNULL(oil_ytd_volume,        '${N}', CAST(oil_ytd_volume AS VARCHAR)),        '${D}') AS m_oil_ytd_volume,
        LIST(IFNULL(primary_allowable,     '${N}', CAST(primary_allowable AS VARCHAR)),     '${D}') AS m_primary_allowable,
        LIST(IFNULL(primary_product,       '${N}', CAST(primary_product AS VARCHAR)),       '${D}') AS m_primary_product,
        LIST(IFNULL(prod_time,             '${N}', CAST(prod_time AS VARCHAR)),             '${D}') AS m_prod_time,
        LIST(IFNULL(prod_time_unit_uom,    '${N}', prod_time_unit_uom),                     '${D}') AS m_prod_time_unit_uom,
        LIST(IFNULL(prod_time_uom,         '${N}', prod_time_uom),                          '${D}') AS m_prod_time_uom,
        LIST(IFNULL(row_changed_date,      '${N}', CAST(row_changed_date AS VARCHAR)),      '${D}') AS m_row_changed_date,
        LIST(IFNULL(sulphur_cum_volume,    '${N}', CAST(sulphur_cum_volume AS VARCHAR)),    '${D}') AS m_sulphur_cum_volume,
        LIST(IFNULL(sulphur_volume,        '${N}', CAST(sulphur_volume AS VARCHAR)),        '${D}') AS m_sulphur_volume,
        LIST(IFNULL(sulphur_volume_uom,    '${N}', sulphur_volume_uom),                     '${D}') AS m_sulphur_volume_uom,
        LIST(IFNULL(sulphur_ytd_volume,    '${N}', CAST(sulphur_ytd_volume AS VARCHAR)),    '${D}') AS m_sulphur_ytd_volume,
        LIST(IFNULL(vol_period,            '${N}', CAST(vol_period AS VARCHAR)),            '${D}') AS m_vol_period,
        LIST(IFNULL(volume_month,          '${N}', CAST(volume_month AS VARCHAR)),          '${D}') AS m_volume_month,
        LIST(IFNULL(volume_year,           '${N}', CAST(volume_year AS VARCHAR)),           '${D}') AS m_volume_year,
        LIST(IFNULL(water_cum_volume,      '${N}', CAST(water_cum_volume AS VARCHAR)),      '${D}') AS m_water_cum_volume,
        LIST(IFNULL(water_volume,          '${N}', CAST(water_volume AS VARCHAR)),          '${D}') AS m_water_volume,
        LIST(IFNULL(water_ytd_volume,      '${N}', CAST(water_ytd_volume AS VARCHAR)),      '${D}') AS m_water_ytd_volume
      FROM gx_pden_vol_sum_by_month  
      GROUP BY m_uwi, r_source
    )
    SELECT 
      w.*,
      m.*,
      p.activity_type                    AS p_activity_type,
      p.average_co2_volume               AS p_average_co2_volume,
      p.average_gas_volume               AS p_average_gas_volume,
      p.average_injection_volume         AS p_average_injection_volume,
      p.average_ngl_volume               AS p_average_ngl_volume,
      p.average_nitrogen_volume          AS p_average_nitrogen_volume,
      p.average_oil_volume               AS p_average_oil_volume,
      p.average_sulphur_volume           AS p_average_sulphur_volume,
      p.average_total_fluids_volume      AS p_average_total_fluids_volume,
      p.average_water_volume             AS p_average_water_volume,
      p.cumulative_co2_volume            AS p_cumulative_co2_volume,
      p.cumulative_gas_volume            AS p_cumulative_gas_volume,
      p.cumulative_injection_volume      AS p_cumulative_injection_volume,
      p.cumulative_ngl_volume            AS p_cumulative_ngl_volume,
      p.cumulative_nitrogen_volume       AS p_cumulative_nitrogen_volume,
      p.cumulative_oil_volume            AS p_cumulative_oil_volume,
      p.cumulative_sulphur_volume        AS p_cumulative_sulphur_volume,
      p.cumulative_total_fluids_volume   AS p_cumulative_total_fluids_volume,
      p.cumulative_water_volume          AS p_cumulative_water_volume,
      p.first_co2_volume                 AS p_first_co2_volume,
      p.first_gas_volume                 AS p_first_gas_volume,
      p.first_gas_volume_date            AS p_first_gas_volume_date,
      p.first_injection_volume           AS p_first_injection_volume,
      p.first_ngl_volume                 AS p_first_ngl_volume,
      p.first_nitrogen_volume            AS p_first_nitrogen_volume,
      p.first_oil_volume                 AS p_first_oil_volume,
      p.first_oil_volume_date            AS p_first_oil_volume_date,
      p.first_sulphur_volume             AS p_first_sulphur_volume,
      p.first_total_fluids_date          AS p_first_total_fluids_date,
      p.first_total_fluids_volume        AS p_first_total_fluids_volume,
      p.first_water_volume               AS p_first_water_volume,
      p.first_water_volume_date          AS p_first_water_volume_date,
      p.gas_eur                          AS p_gas_eur,
      p.gas_reserves                     AS p_gas_reserves,
      p.gx_percent_allocation            AS p_gx_percent_allocation,
      p.last_co2_volume                  AS p_last_co2_volume,
      p.last_gas_volume                  AS p_last_gas_volume,
      p.last_gas_volume_date             AS p_last_gas_volume_date,
      p.last_injection_volume            AS p_last_injection_volume,
      p.last_ngl_volume                  AS p_last_ngl_volume,
      p.last_nitrogen_volume             AS p_last_nitrogen_volume,
      p.last_oil_volume                  AS p_last_oil_volume,
      p.last_oil_volume_date             AS p_last_oil_volume_date,
      p.last_sulphur_volume              AS p_last_sulphur_volume,
      p.last_total_fluids_date           AS p_last_total_fluids_date,
      p.last_total_fluids_volume         AS p_last_total_fluids_volume,
      p.last_water_volume                AS p_last_water_volume,
      p.last_water_volume_date           AS p_last_water_volume_date,
      p.max_co2_volume                   AS p_max_co2_volume,
      p.max_gas_volume                   AS p_max_gas_volume,
      p.max_injection_volume             AS p_max_injection_volume,
      p.max_ngl_volume                   AS p_max_ngl_volume,
      p.max_nitrogen_volume              AS p_max_nitrogen_volume,
      p.max_oil_volume                   AS p_max_oil_volume,
      p.max_sulphur_volume               AS p_max_sulphur_volume,
      p.max_total_fluids_volume          AS p_max_total_fluids_volume,
      p.max_water_volume                 AS p_max_water_volume,
      p.min_co2_volume                   AS p_min_co2_volume,
      p.min_gas_volume                   AS p_min_gas_volume,
      p.min_injection_volume             AS p_min_injection_volume,
      p.min_ngl_volume                   AS p_min_ngl_volume,
      p.min_nitrogen_volume              AS p_min_nitrogen_volume,
      p.min_oil_volume                   AS p_min_oil_volume,
      p.min_sulphur_volume               AS p_min_sulphur_volume,
      p.min_total_fluids_volume          AS p_min_total_fluids_volume,
      p.min_water_volume                 AS p_min_water_volume,
      p.oil_eur                          AS p_oil_eur,
      p.oil_reserves                     AS p_oil_reserves,
      p.row_changed_date                 AS p_row_changed_date,
      p.uwi                              AS p_uwi,
      p.volume_method                    AS p_volume_method,
      p.zone_id                          AS p_zone_id
    FROM well_cumulative_production p     
    JOIN w ON
      p.uwi = w.w_uwi
    JOIN m ON
      p.uwi = m.m_uwi
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
  let { func, key, typ, arg, obj } = args;

  const ensureType = (type: string, val: any) => {
    if (val == null) {
      return null;
    } else if (type === "object") {
      console.log("UNEXPECTED OBJECT TYPE! (needs xformer)", type);
      console.log(val);
      return null;
    } else if (type === "string") {
      //return decodeWin1252(val)
      return val.replace(/[\u0000-\u001F\u007F-\u009F]/g, "");
    } else if (type === "number") {
      // cuz blank strings (\t\r\n) evaluate to 0
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

  const D = "|&|";
  const N = "purrNULL";

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
  w_assigned_field: {
    ts_type: "string",
  },
  w_common_well_name: {
    ts_type: "string",
  },
  w_completion_date: {
    ts_type: "date",
  },
  w_country: {
    ts_type: "string",
  },
  w_county: {
    ts_type: "string",
  },
  w_current_class: {
    ts_type: "string",
  },
  w_current_status: {
    ts_type: "string",
  },
  w_depth_datum: {
    ts_type: "number",
  },
  w_final_td: {
    ts_type: "number",
  },
  w_ground_elev: {
    ts_type: "number",
  },
  w_kb_elev: {
    ts_type: "number",
  },
  w_lease_name: {
    ts_type: "string",
  },
  w_operator: {
    ts_type: "string",
  },
  w_province_state: {
    ts_type: "string",
  },
  w_row_changed_date: {
    ts_type: "date",
  },
  w_spud_date: {
    ts_type: "date",
  },
  w_surface_latitude: {
    ts_type: "number",
  },
  w_surface_longitude: {
    ts_type: "number",
  },
  w_td_form: {
    ts_type: "string",
  },
  w_well_name: {
    ts_type: "string",
  },
  w_well_number: {
    ts_type: "string",
  },

  // GX_PDEN_VOL_SUM_BY_MONTH

  m_uwi: {
    ts_type: "string",
  },
  a_m_uwi: {
    ts_type: "string",
  },
  m_activity_type: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  m_pden_date: {
    ts_type: "date",
    xform: "delimited_array_with_nulls",
  },
  m_pden_source: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  m_pden_type: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  m_volume_method: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  m_zone_id: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  m_boe_cum_volume: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_boe_volume: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_boe_ytd_volume: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_co2_cum_volume: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_co2_volume: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_co2_ytd_volume: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_cum_flag: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_gas_cum_volume: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_gas_quality: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_gas_quality_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  m_gas_volume: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_gas_ytd_volume: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_gx_percent_allocation: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_injection_cum_volume: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_injection_pressure: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_injection_product: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_injection_volume: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_injection_volume_uom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  m_injection_ytd_volume: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_ngl_cum_volume: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_ngl_volume: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_ngl_volume_uom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  m_ngl_ytd_volume: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_nitrogen_cum_volume: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_nitrogen_volume: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_nitrogen_ytd_volume: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_no_of_gas_wells: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_no_of_inj_wells: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_no_of_oil_wells: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_oil_cum_volume: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_oil_quality: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_oil_quality_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  m_oil_volume: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_oil_ytd_volume: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_primary_allowable: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  m_primary_product: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  m_prod_time: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_prod_time_unit_uom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  m_prod_time_uom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  m_row_changed_date: {
    ts_type: "date",
    xform: "delimited_array_with_nulls",
  },
  m_sulphur_cum_volume: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_sulphur_volume: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_sulphur_volume_uom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  m_sulphur_ytd_volume: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_vol_period: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  m_volume_month: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_volume_year: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_water_cum_volume: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_water_volume: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_water_ytd_volume: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },

  // WELL_CUMULATIVE_PRODUCTION

  p_activity_type: {
    ts_type: "string",
  },
  p_average_co2_volume: {
    ts_type: "number",
  },
  p_average_gas_volume: {
    ts_type: "number",
  },
  p_average_injection_volume: {
    ts_type: "number",
  },
  p_average_ngl_volume: {
    ts_type: "number",
  },
  p_average_nitrogen_volume: {
    ts_type: "number",
  },
  p_average_oil_volume: {
    ts_type: "number",
  },
  p_average_sulphur_volume: {
    ts_type: "number",
  },
  p_average_total_fluids_volume: {
    ts_type: "number",
  },
  p_average_water_volume: {
    ts_type: "number",
  },
  p_cumulative_co2_volume: {
    ts_type: "number",
  },
  p_cumulative_gas_volume: {
    ts_type: "number",
  },
  p_cumulative_injection_volume: {
    ts_type: "number",
  },
  p_cumulative_ngl_volume: {
    ts_type: "number",
  },
  p_cumulative_nitrogen_volume: {
    ts_type: "number",
  },
  p_cumulative_oil_volume: {
    ts_type: "number",
  },
  p_cumulative_sulphur_volume: {
    ts_type: "number",
  },
  p_cumulative_total_fluids_volume: {
    ts_type: "number",
  },
  p_cumulative_water_volume: {
    ts_type: "number",
  },
  p_first_co2_volume: {
    ts_type: "number",
  },
  p_first_gas_volume: {
    ts_type: "number",
  },
  p_first_gas_volume_date: {
    ts_type: "date",
  },
  p_first_injection_volume: {
    ts_type: "number",
  },
  p_first_ngl_volume: {
    ts_type: "number",
  },
  p_first_nitrogen_volume: {
    ts_type: "number",
  },
  p_first_oil_volume: {
    ts_type: "number",
  },
  p_first_oil_volume_date: {
    ts_type: "date",
  },
  p_first_sulphur_volume: {
    ts_type: "number",
  },
  p_first_total_fluids_date: {
    ts_type: "date",
  },
  p_first_total_fluids_volume: {
    ts_type: "number",
  },
  p_first_water_volume: {
    ts_type: "number",
  },
  p_first_water_volume_date: {
    ts_type: "date",
  },
  p_gas_eur: {
    ts_type: "number",
  },
  p_gas_reserves: {
    ts_type: "number",
  },
  p_gx_percent_allocation: {
    ts_type: "number",
  },
  p_last_co2_volume: {
    ts_type: "number",
  },
  p_last_gas_volume: {
    ts_type: "number",
  },
  p_last_gas_volume_date: {
    ts_type: "date",
  },
  p_last_injection_volume: {
    ts_type: "number",
  },
  p_last_ngl_volume: {
    ts_type: "number",
  },
  p_last_nitrogen_volume: {
    ts_type: "number",
  },
  p_last_oil_volume: {
    ts_type: "number",
  },
  p_last_oil_volume_date: {
    ts_type: "date",
  },
  p_last_sulphur_volume: {
    ts_type: "number",
  },
  p_last_total_fluids_date: {
    ts_type: "date",
  },
  p_last_total_fluids_volume: {
    ts_type: "number",
  },
  p_last_water_volume: {
    ts_type: "number",
  },
  p_last_water_volume_date: {
    ts_type: "date",
  },
  p_max_co2_volume: {
    ts_type: "number",
  },
  p_max_gas_volume: {
    ts_type: "number",
  },
  p_max_injection_volume: {
    ts_type: "number",
  },
  p_max_ngl_volume: {
    ts_type: "number",
  },
  p_max_nitrogen_volume: {
    ts_type: "number",
  },
  p_max_oil_volume: {
    ts_type: "number",
  },
  p_max_sulphur_volume: {
    ts_type: "number",
  },
  p_max_total_fluids_volume: {
    ts_type: "number",
  },
  p_max_water_volume: {
    ts_type: "number",
  },
  p_min_co2_volume: {
    ts_type: "number",
  },
  p_min_gas_volume: {
    ts_type: "number",
  },
  p_min_injection_volume: {
    ts_type: "number",
  },
  p_min_ngl_volume: {
    ts_type: "number",
  },
  p_min_nitrogen_volume: {
    ts_type: "number",
  },
  p_min_oil_volume: {
    ts_type: "number",
  },
  p_min_sulphur_volume: {
    ts_type: "number",
  },
  p_min_total_fluids_volume: {
    ts_type: "number",
  },
  p_min_water_volume: {
    ts_type: "number",
  },
  p_oil_eur: {
    ts_type: "number",
  },
  p_oil_reserves: {
    ts_type: "number",
  },
  p_row_changed_date: {
    ts_type: "date",
  },
  p_uwi: {
    ts_type: "string",
  },
  p_volume_method: {
    ts_type: "string",
  },
  p_zone_id: {
    ts_type: "string",
  },

  // R_SOURCE

  r_source: {
    ts_type: "string",
  },
};

const prefixes = {
  w_: "well",
  m_: "gx_pden_vol_sum_by_month",
  p_: "well_cumulative_production",
  r_: "r_source",
};

const global_id_keys = ["w_uwi"];

const well_id_keys = ["w_uwi"];

const pg_cols = ["id", "repo_id", "well_id", "geo_type", "tag", "doc"];

const default_chunk = 500;

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
