import { serialize } from "https://deno.land/x/serialize_javascript/mod.ts";

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

  // let select = `SELECT * FROM (
  //   WITH w AS (
  //     SELECT
  //       uwi                              AS w_uwi
  //     FROM well
  //   ),
  //   p AS (
  //     SELECT
  //       activity_type                    AS p_activity_type,
  //       average_co2_volume               AS p_average_co2_volume,
  //       average_gas_volume               AS p_average_gas_volume,
  //       average_injection_volume         AS p_average_injection_volume,
  //       average_ngl_volume               AS p_average_ngl_volume,
  //       average_nitrogen_volume          AS p_average_nitrogen_volume,
  //       average_oil_volume               AS p_average_oil_volume,
  //       average_sulphur_volume           AS p_average_sulphur_volume,
  //       average_total_fluids_volume      AS p_average_total_fluids_volume,
  //       average_water_volume             AS p_average_water_volume,
  //       cumulative_co2_volume            AS p_cumulative_co2_volume,
  //       cumulative_gas_volume            AS p_cumulative_gas_volume,
  //       cumulative_injection_volume      AS p_cumulative_injection_volume,
  //       cumulative_ngl_volume            AS p_cumulative_ngl_volume,
  //       cumulative_nitrogen_volume       AS p_cumulative_nitrogen_volume,
  //       cumulative_oil_volume            AS p_cumulative_oil_volume,
  //       cumulative_sulphur_volume        AS p_cumulative_sulphur_volume,
  //       cumulative_total_fluids_volume   AS p_cumulative_total_fluids_volume,
  //       cumulative_water_volume          AS p_cumulative_water_volume,
  //       first_co2_volume                 AS p_first_co2_volume,
  //       first_gas_volume                 AS p_first_gas_volume,
  //       first_gas_volume_date            AS p_first_gas_volume_date,
  //       first_injection_volume           AS p_first_injection_volume,
  //       first_ngl_volume                 AS p_first_ngl_volume,
  //       first_nitrogen_volume            AS p_first_nitrogen_volume,
  //       first_oil_volume                 AS p_first_oil_volume,
  //       first_oil_volume_date            AS p_first_oil_volume_date,
  //       first_sulphur_volume             AS p_first_sulphur_volume,
  //       first_total_fluids_date          AS p_first_total_fluids_date,
  //       first_total_fluids_volume        AS p_first_total_fluids_volume,
  //       first_water_volume               AS p_first_water_volume,
  //       first_water_volume_date          AS p_first_water_volume_date,
  //       gas_eur                          AS p_gas_eur,
  //       gas_reserves                     AS p_gas_reserves,
  //       gx_percent_allocation            AS p_gx_percent_allocation,
  //       last_co2_volume                  AS p_last_co2_volume,
  //       last_gas_volume                  AS p_last_gas_volume,
  //       last_gas_volume_date             AS p_last_gas_volume_date,
  //       last_injection_volume            AS p_last_injection_volume,
  //       last_ngl_volume                  AS p_last_ngl_volume,
  //       last_nitrogen_volume             AS p_last_nitrogen_volume,
  //       last_oil_volume                  AS p_last_oil_volume,
  //       last_oil_volume_date             AS p_last_oil_volume_date,
  //       last_sulphur_volume              AS p_last_sulphur_volume,
  //       last_total_fluids_date           AS p_last_total_fluids_date,
  //       last_total_fluids_volume         AS p_last_total_fluids_volume,
  //       last_water_volume                AS p_last_water_volume,
  //       last_water_volume_date           AS p_last_water_volume_date,
  //       max_co2_volume                   AS p_max_co2_volume,
  //       max_gas_volume                   AS p_max_gas_volume,
  //       max_injection_volume             AS p_max_injection_volume,
  //       max_ngl_volume                   AS p_max_ngl_volume,
  //       max_nitrogen_volume              AS p_max_nitrogen_volume,
  //       max_oil_volume                   AS p_max_oil_volume,
  //       max_sulphur_volume               AS p_max_sulphur_volume,
  //       max_total_fluids_volume          AS p_max_total_fluids_volume,
  //       max_water_volume                 AS p_max_water_volume,
  //       min_co2_volume                   AS p_min_co2_volume,
  //       min_gas_volume                   AS p_min_gas_volume,
  //       min_injection_volume             AS p_min_injection_volume,
  //       min_ngl_volume                   AS p_min_ngl_volume,
  //       min_nitrogen_volume              AS p_min_nitrogen_volume,
  //       min_oil_volume                   AS p_min_oil_volume,
  //       min_sulphur_volume               AS p_min_sulphur_volume,
  //       min_total_fluids_volume          AS p_min_total_fluids_volume,
  //       min_water_volume                 AS p_min_water_volume,
  //       oil_eur                          AS p_oil_eur,
  //       oil_reserves                     AS p_oil_reserves,
  //       row_changed_date                 AS p_row_changed_date,
  //       uwi                              AS p_uwi,
  //       volume_method                    AS p_volume_method,
  //       zone_id                          AS p_zone_id
  //     FROM well_cumulative_production
  //   ),
  //   m AS (
  //     SELECT
  //       uwi                              AS id_m_uwi,
  //       zone_id                          AS id_m_zone_id,
  //       activity_type                    AS id_m_activity_type,
  //       volume_method                    AS id_m_volume_method,
  //       LIST(IFNULL(uwi,                   '${N}', uwi),                                    '${D}') AS m_uwi,
  //       LIST(IFNULL(activity_type,         '${N}', activity_type),                          '${D}') AS m_activity_type,
  //       LIST(IFNULL(pden_date,             '${N}', CAST(pden_date AS VARCHAR)),             '${D}') AS m_pden_date,
  //       LIST(IFNULL(pden_source,           '${N}', pden_source),                            '${D}') AS m_pden_source,
  //       LIST(IFNULL(pden_type,             '${N}', pden_type),                              '${D}') AS m_pden_type,
  //       LIST(IFNULL(volume_method,         '${N}', volume_method),                          '${D}') AS m_volume_method,
  //       LIST(IFNULL(zone_id,               '${N}', zone_id),                                '${D}') AS m_zone_id,
  //       LIST(IFNULL(boe_cum_volume,        '${N}', CAST(boe_cum_volume AS VARCHAR)),        '${D}') AS m_boe_cum_volume,
  //       LIST(IFNULL(boe_volume,            '${N}', CAST(boe_volume AS VARCHAR)),            '${D}') AS m_boe_volume,
  //       LIST(IFNULL(boe_ytd_volume,        '${N}', CAST(boe_ytd_volume AS VARCHAR)),        '${D}') AS m_boe_ytd_volume,
  //       LIST(IFNULL(co2_cum_volume,        '${N}', CAST(co2_cum_volume AS VARCHAR)),        '${D}') AS m_co2_cum_volume,
  //       LIST(IFNULL(co2_volume,            '${N}', CAST(co2_volume AS VARCHAR)),            '${D}') AS m_co2_volume,
  //       LIST(IFNULL(co2_ytd_volume,        '${N}', CAST(co2_ytd_volume AS VARCHAR)),        '${D}') AS m_co2_ytd_volume,
  //       LIST(IFNULL(cum_flag,              '${N}', CAST(cum_flag AS VARCHAR)),              '${D}') AS m_cum_flag,
  //       LIST(IFNULL(gas_cum_volume,        '${N}', CAST(gas_cum_volume AS VARCHAR)),        '${D}') AS m_gas_cum_volume,
  //       LIST(IFNULL(gas_quality,           '${N}', CAST(gas_quality AS VARCHAR)),           '${D}') AS m_gas_quality,
  //       LIST(IFNULL(gas_quality_ouom,      '${N}', gas_quality_ouom),                       '${D}') AS m_gas_quality_ouom,
  //       LIST(IFNULL(gas_volume,            '${N}', CAST(gas_volume AS VARCHAR)),            '${D}') AS m_gas_volume,
  //       LIST(IFNULL(gas_ytd_volume,        '${N}', CAST(gas_ytd_volume AS VARCHAR)),        '${D}') AS m_gas_ytd_volume,
  //       LIST(IFNULL(gx_percent_allocation, '${N}', CAST(gx_percent_allocation AS VARCHAR)), '${D}') AS m_gx_percent_allocation,
  //       LIST(IFNULL(injection_cum_volume,  '${N}', CAST(injection_cum_volume AS VARCHAR)),  '${D}') AS m_injection_cum_volume,
  //       LIST(IFNULL(injection_pressure,    '${N}', CAST(injection_pressure AS VARCHAR)),    '${D}') AS m_injection_pressure,
  //       LIST(IFNULL(injection_product,     '${N}', CAST(injection_product AS VARCHAR)),     '${D}') AS m_injection_product,
  //       LIST(IFNULL(injection_volume,      '${N}', CAST(injection_volume AS VARCHAR)),      '${D}') AS m_injection_volume,
  //       LIST(IFNULL(injection_volume_uom,  '${N}', injection_volume_uom),                   '${D}') AS m_injection_volume_uom,
  //       LIST(IFNULL(injection_ytd_volume,  '${N}', CAST(injection_ytd_volume AS VARCHAR)),  '${D}') AS m_injection_ytd_volume,
  //       LIST(IFNULL(ngl_cum_volume,        '${N}', CAST(ngl_cum_volume AS VARCHAR)),        '${D}') AS m_ngl_cum_volume,
  //       LIST(IFNULL(ngl_volume,            '${N}', CAST(ngl_volume AS VARCHAR)),            '${D}') AS m_ngl_volume,
  //       LIST(IFNULL(ngl_volume_uom,        '${N}', ngl_volume_uom),                         '${D}') AS m_ngl_volume_uom,
  //       LIST(IFNULL(ngl_ytd_volume,        '${N}', CAST(ngl_ytd_volume AS VARCHAR)),        '${D}') AS m_ngl_ytd_volume,
  //       LIST(IFNULL(nitrogen_cum_volume,   '${N}', CAST(nitrogen_cum_volume AS VARCHAR)),   '${D}') AS m_nitrogen_cum_volume,
  //       LIST(IFNULL(nitrogen_volume,       '${N}', CAST(nitrogen_volume AS VARCHAR)),       '${D}') AS m_nitrogen_volume,
  //       LIST(IFNULL(nitrogen_ytd_volume,   '${N}', CAST(nitrogen_ytd_volume AS VARCHAR)),   '${D}') AS m_nitrogen_ytd_volume,
  //       LIST(IFNULL(no_of_gas_wells,       '${N}', CAST(no_of_gas_wells AS VARCHAR)),       '${D}') AS m_no_of_gas_wells,
  //       LIST(IFNULL(no_of_inj_wells,       '${N}', CAST(no_of_inj_wells AS VARCHAR)),       '${D}') AS m_no_of_inj_wells,
  //       LIST(IFNULL(no_of_oil_wells,       '${N}', CAST(no_of_oil_wells AS VARCHAR)),       '${D}') AS m_no_of_oil_wells,
  //       LIST(IFNULL(oil_cum_volume,        '${N}', CAST(oil_cum_volume AS VARCHAR)),        '${D}') AS m_oil_cum_volume,
  //       LIST(IFNULL(oil_quality,           '${N}', CAST(oil_quality AS VARCHAR)),           '${D}') AS m_oil_quality,
  //       LIST(IFNULL(oil_quality_ouom,      '${N}', oil_quality_ouom),                       '${D}') AS m_oil_quality_ouom,
  //       LIST(IFNULL(oil_volume,            '${N}', CAST(oil_volume AS VARCHAR)),            '${D}') AS m_oil_volume,
  //       LIST(IFNULL(oil_ytd_volume,        '${N}', CAST(oil_ytd_volume AS VARCHAR)),        '${D}') AS m_oil_ytd_volume,
  //       LIST(IFNULL(primary_allowable,     '${N}', CAST(primary_allowable AS VARCHAR)),     '${D}') AS m_primary_allowable,
  //       LIST(IFNULL(primary_product,       '${N}', CAST(primary_product AS VARCHAR)),       '${D}') AS m_primary_product,
  //       LIST(IFNULL(prod_time,             '${N}', CAST(prod_time AS VARCHAR)),             '${D}') AS m_prod_time,
  //       LIST(IFNULL(prod_time_unit_uom,    '${N}', prod_time_unit_uom),                     '${D}') AS m_prod_time_unit_uom,
  //       LIST(IFNULL(prod_time_uom,         '${N}', prod_time_uom),                          '${D}') AS m_prod_time_uom,
  //       LIST(IFNULL(row_changed_date,      '${N}', CAST(row_changed_date AS VARCHAR)),      '${D}') AS m_row_changed_date,
  //       LIST(IFNULL(sulphur_cum_volume,    '${N}', CAST(sulphur_cum_volume AS VARCHAR)),    '${D}') AS m_sulphur_cum_volume,
  //       LIST(IFNULL(sulphur_volume,        '${N}', CAST(sulphur_volume AS VARCHAR)),        '${D}') AS m_sulphur_volume,
  //       LIST(IFNULL(sulphur_volume_uom,    '${N}', sulphur_volume_uom),                     '${D}') AS m_sulphur_volume_uom,
  //       LIST(IFNULL(sulphur_ytd_volume,    '${N}', CAST(sulphur_ytd_volume AS VARCHAR)),    '${D}') AS m_sulphur_ytd_volume,
  //       LIST(IFNULL(vol_period,            '${N}', CAST(vol_period AS VARCHAR)),            '${D}') AS m_vol_period,
  //       LIST(IFNULL(volume_month,          '${N}', CAST(volume_month AS VARCHAR)),          '${D}') AS m_volume_month,
  //       LIST(IFNULL(volume_year,           '${N}', CAST(volume_year AS VARCHAR)),           '${D}') AS m_volume_year,
  //       LIST(IFNULL(water_cum_volume,      '${N}', CAST(water_cum_volume AS VARCHAR)),      '${D}') AS m_water_cum_volume,
  //       LIST(IFNULL(water_volume,          '${N}', CAST(water_volume AS VARCHAR)),          '${D}') AS m_water_volume,
  //       LIST(IFNULL(water_ytd_volume,      '${N}', CAST(water_ytd_volume AS VARCHAR)),      '${D}') AS m_water_ytd_volume
  //     FROM gx_pden_vol_sum_by_month
  //     GROUP BY m_uwi, r_source
  //   )
  //   SELECT
  //     w.*,
  //     m.*,
  //     p.*
  //   FROM w
  //   JOIN p ON
  //     p.p_uwi = w.w_uwi
  //   JOIN m ON
  //     m.m_uwi = w.w_uwi AND
  //     m.id_m_zone_id = p.p_zone_id AND
  //     m.id_m_activity_type = p.p_activity_type AND
  //     m.id_m_volume_method = p.p_volume_method
  //   ) x`;

  let select = `SELECT * FROM (
      WITH w AS (
      SELECT
        uwi                          AS w_uwi
      FROM well
    ),
    p AS (
      SELECT
      activity_type                    AS p_activity_type,
      average_gas_volume               AS p_average_gas_volume,
      average_injection_volume         AS p_average_injection_volume,
      average_oil_volume               AS p_average_oil_volume,
      average_total_fluids_volume      AS p_average_total_fluids_volume,
      average_water_volume             AS p_average_water_volume,
      cumulative_gas_volume            AS p_cumulative_gas_volume,
      cumulative_injection_volume      AS p_cumulative_injection_volume,
      cumulative_oil_volume            AS p_cumulative_oil_volume,
      cumulative_total_fluids_volume   AS p_cumulative_total_fluids_volume,
      cumulative_water_volume          AS p_cumulative_water_volume,
      first_gas_volume                 AS p_first_gas_volume,
      first_gas_volume_date            AS p_first_gas_volume_date,
      first_injection_volume           AS p_first_injection_volume,
      first_oil_volume                 AS p_first_oil_volume,
      first_oil_volume_date            AS p_first_oil_volume_date,
      first_total_fluids_date          AS p_first_total_fluids_date,
      first_total_fluids_volume        AS p_first_total_fluids_volume,
      first_water_volume               AS p_first_water_volume,
      first_water_volume_date          AS p_first_water_volume_date,
      gas_eur                          AS p_gas_eur,
      gas_reserves                     AS p_gas_reserves,
      gx_percent_allocation            AS p_gx_percent_allocation,
      last_gas_volume                  AS p_last_gas_volume,
      last_gas_volume_date             AS p_last_gas_volume_date,
      last_injection_volume            AS p_last_injection_volume,
      last_oil_volume                  AS p_last_oil_volume,
      last_oil_volume_date             AS p_last_oil_volume_date,
      last_total_fluids_date           AS p_last_total_fluids_date,
      last_total_fluids_volume         AS p_last_total_fluids_volume,
      last_water_volume                AS p_last_water_volume,
      last_water_volume_date           AS p_last_water_volume_date,
      max_gas_volume                   AS p_max_gas_volume,
      max_injection_volume             AS p_max_injection_volume,
      max_oil_volume                   AS p_max_oil_volume,
      max_total_fluids_volume          AS p_max_total_fluids_volume,
      max_water_volume                 AS p_max_water_volume,
      min_gas_volume                   AS p_min_gas_volume,
      min_injection_volume             AS p_min_injection_volume,
      min_oil_volume                   AS p_min_oil_volume,
      min_total_fluids_volume          AS p_min_total_fluids_volume,
      min_water_volume                 AS p_min_water_volume,
      oil_eur                          AS p_oil_eur,
      oil_reserves                     AS p_oil_reserves,
      row_changed_date                 AS p_row_changed_date,
      uwi                              AS p_uwi,
      volume_method                    AS p_volume_method,
      zone_id                          AS p_zone_id
      FROM well_cumulative_production
      ${whereRecent}
    ),
    m AS (
      SELECT
        uwi                              AS id_m_uwi,
        zone_id                          AS id_m_zone_id,
        activity_type                    AS id_m_activity_type,
        volume_method                    AS id_m_volume_method,
        MAX(row_changed_date)            AS max_row_changed_date,
        LIST(IFNULL(pden_date,             '${N}', CAST(pden_date AS VARCHAR)),             '${D}') AS m_pden_date,
        LIST(IFNULL(pden_source,           '${N}', pden_source),                            '${D}') AS m_pden_source,
        LIST(IFNULL(gas_volume,            '${N}', CAST(gas_volume AS VARCHAR)),            '${D}') AS m_gas_volume,
        LIST(IFNULL(gx_percent_allocation, '${N}', CAST(gx_percent_allocation AS VARCHAR)), '${D}') AS m_gx_percent_allocation,
        LIST(IFNULL(oil_volume,            '${N}', CAST(oil_volume AS VARCHAR)),            '${D}') AS m_oil_volume,
        LIST(IFNULL(prod_time,             '${N}', CAST(prod_time AS VARCHAR)),             '${D}') AS m_prod_time,
        LIST(IFNULL(row_changed_date,      '${N}', CAST(row_changed_date AS VARCHAR)),      '${D}') AS m_row_changed_date,
        LIST(IFNULL(volume_month,          '${N}', CAST(volume_month AS VARCHAR)),          '${D}') AS m_volume_month,
        LIST(IFNULL(volume_year,           '${N}', CAST(volume_year AS VARCHAR)),           '${D}') AS m_volume_year,
        LIST(IFNULL(water_volume,          '${N}', CAST(water_volume AS VARCHAR)),          '${D}') AS m_water_volume
      FROM gx_pden_vol_sum_by_month  
      GROUP BY uwi, zone_id, activity_type, volume_method
    )
    SELECT 
      w.*,
      m.*,
      p.*
    FROM w   
    JOIN p ON
      p.p_uwi = w.w_uwi
    JOIN m ON
      m.id_m_uwi = w.w_uwi AND
      m.id_m_zone_id = p.p_zone_id AND
      m.id_m_activity_type = p.p_activity_type AND
      m.id_m_volume_method = p.p_volume_method
    ) x`;

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

  // GX_PDEN_VOL_SUM_BY_MONTH

  id_m_uwi: {
    ts_type: "string",
  },
  id_m_zone_id: {
    ts_type: "string",
  },
  id_m_activity_type: {
    ts_type: "string",
  },
  id_m_volume_method: {
    ts_type: "string",
  },
  max_row_changed_date: {
    ts_type: "date",
  },
  // m_uwi: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_activity_type: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  m_pden_date: {
    ts_type: "date",
    xform: "delimited_array_with_nulls",
  },
  m_pden_source: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  // m_pden_type: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_volume_method: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_zone_id: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_boe_cum_volume: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_boe_volume: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_boe_ytd_volume: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_co2_cum_volume: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_co2_volume: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_co2_ytd_volume: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_cum_flag: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_gas_cum_volume: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_gas_quality: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_gas_quality_ouom: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  m_gas_volume: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  // m_gas_ytd_volume: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  m_gx_percent_allocation: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  // m_injection_cum_volume: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_injection_pressure: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_injection_product: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_injection_volume: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_injection_volume_uom: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_injection_ytd_volume: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_ngl_cum_volume: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_ngl_volume: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_ngl_volume_uom: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_ngl_ytd_volume: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_nitrogen_cum_volume: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_nitrogen_volume: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_nitrogen_ytd_volume: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_no_of_gas_wells: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_no_of_inj_wells: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_no_of_oil_wells: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_oil_cum_volume: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_oil_quality: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_oil_quality_ouom: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  m_oil_volume: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  // m_oil_ytd_volume: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_primary_allowable: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_primary_product: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  m_prod_time: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  // m_prod_time_unit_uom: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_prod_time_uom: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  m_row_changed_date: {
    ts_type: "date",
    xform: "delimited_array_with_nulls",
  },
  // m_sulphur_cum_volume: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_sulphur_volume: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_sulphur_volume_uom: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_sulphur_ytd_volume: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_vol_period: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  m_volume_month: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_volume_year: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  // m_water_cum_volume: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  m_water_volume: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  // m_water_ytd_volume: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },

  // WELL_CUMULATIVE_PRODUCTION

  p_activity_type: {
    ts_type: "string",
  },
  // p_average_co2_volume: {
  //   ts_type: "number",
  // },
  p_average_gas_volume: {
    ts_type: "number",
  },
  p_average_injection_volume: {
    ts_type: "number",
  },
  // p_average_ngl_volume: {
  //   ts_type: "number",
  // },
  // p_average_nitrogen_volume: {
  //   ts_type: "number",
  // },
  p_average_oil_volume: {
    ts_type: "number",
  },
  // p_average_sulphur_volume: {
  //   ts_type: "number",
  // },
  p_average_total_fluids_volume: {
    ts_type: "number",
  },
  p_average_water_volume: {
    ts_type: "number",
  },
  // p_cumulative_co2_volume: {
  //   ts_type: "number",
  // },
  p_cumulative_gas_volume: {
    ts_type: "number",
  },
  p_cumulative_injection_volume: {
    ts_type: "number",
  },
  // p_cumulative_ngl_volume: {
  //   ts_type: "number",
  // },
  // p_cumulative_nitrogen_volume: {
  //   ts_type: "number",
  // },
  p_cumulative_oil_volume: {
    ts_type: "number",
  },
  // p_cumulative_sulphur_volume: {
  //   ts_type: "number",
  // },
  p_cumulative_total_fluids_volume: {
    ts_type: "number",
  },
  p_cumulative_water_volume: {
    ts_type: "number",
  },
  // p_first_co2_volume: {
  //   ts_type: "number",
  // },
  p_first_gas_volume: {
    ts_type: "number",
  },
  p_first_gas_volume_date: {
    ts_type: "date",
  },
  p_first_injection_volume: {
    ts_type: "number",
  },
  // p_first_ngl_volume: {
  //   ts_type: "number",
  // },
  // p_first_nitrogen_volume: {
  //   ts_type: "number",
  // },
  p_first_oil_volume: {
    ts_type: "number",
  },
  p_first_oil_volume_date: {
    ts_type: "date",
  },
  // p_first_sulphur_volume: {
  //   ts_type: "number",
  // },
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
  // p_last_co2_volume: {
  //   ts_type: "number",
  // },
  p_last_gas_volume: {
    ts_type: "number",
  },
  p_last_gas_volume_date: {
    ts_type: "date",
  },
  p_last_injection_volume: {
    ts_type: "number",
  },
  // p_last_ngl_volume: {
  //   ts_type: "number",
  // },
  // p_last_nitrogen_volume: {
  //   ts_type: "number",
  // },
  p_last_oil_volume: {
    ts_type: "number",
  },
  p_last_oil_volume_date: {
    ts_type: "date",
  },
  // p_last_sulphur_volume: {
  //   ts_type: "number",
  // },
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
  // p_max_co2_volume: {
  //   ts_type: "number",
  // },
  p_max_gas_volume: {
    ts_type: "number",
  },
  p_max_injection_volume: {
    ts_type: "number",
  },
  // p_max_ngl_volume: {
  //   ts_type: "number",
  // },
  // p_max_nitrogen_volume: {
  //   ts_type: "number",
  // },
  p_max_oil_volume: {
    ts_type: "number",
  },
  // p_max_sulphur_volume: {
  //   ts_type: "number",
  // },
  p_max_total_fluids_volume: {
    ts_type: "number",
  },
  p_max_water_volume: {
    ts_type: "number",
  },
  // p_min_co2_volume: {
  //   ts_type: "number",
  // },
  p_min_gas_volume: {
    ts_type: "number",
  },
  p_min_injection_volume: {
    ts_type: "number",
  },
  // p_min_ngl_volume: {
  //   ts_type: "number",
  // },
  // p_min_nitrogen_volume: {
  //   ts_type: "number",
  // },
  p_min_oil_volume: {
    ts_type: "number",
  },
  // p_min_sulphur_volume: {
  //   ts_type: "number",
  // },
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

const asset_id_keys = ["w_uwi"];

const well_id_keys = ["w_uwi"];

const default_chunk = 100; // 1000

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
    notes: [
      "Yes, this will be quite slow.",
      "Many fields in GX_PDEN_VOL_SUM_BY_MONTH omitted to conserve memory.",
    ],
  };
};
