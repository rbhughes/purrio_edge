const purr_recent = "__purrRECENT__"
const purr_null = "__purrNULL__"
const purr_delimiter = "__purrDELIMITER__";

const select = `SELECT * FROM (
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
    ${purr_recent}
  ),
  m AS (
    SELECT
      uwi                            AS id_m_uwi,
      zone_id                        AS id_m_zone_id,
      activity_type                  AS id_m_activity_type,
      volume_method                  AS id_m_volume_method,
      MAX(row_changed_date)          AS max_row_changed_date,
      LIST(IFNULL(pden_date,             '${purr_null}', CAST(pden_date AS VARCHAR)),             '${purr_delimiter}') AS m_pden_date,
      LIST(IFNULL(pden_source,           '${purr_null}', pden_source),                            '${purr_delimiter}') AS m_pden_source,
      LIST(IFNULL(gas_volume,            '${purr_null}', CAST(gas_volume AS VARCHAR)),            '${purr_delimiter}') AS m_gas_volume,
      LIST(IFNULL(gx_percent_allocation, '${purr_null}', CAST(gx_percent_allocation AS VARCHAR)), '${purr_delimiter}') AS m_gx_percent_allocation,
      LIST(IFNULL(oil_volume,            '${purr_null}', CAST(oil_volume AS VARCHAR)),            '${purr_delimiter}') AS m_oil_volume,
      LIST(IFNULL(prod_time,             '${purr_null}', CAST(prod_time AS VARCHAR)),             '${purr_delimiter}') AS m_prod_time,
      LIST(IFNULL(row_changed_date,      '${purr_null}', CAST(row_changed_date AS VARCHAR)),      '${purr_delimiter}') AS m_row_changed_date,
      LIST(IFNULL(volume_month,          '${purr_null}', CAST(volume_month AS VARCHAR)),          '${purr_delimiter}') AS m_volume_month,
      LIST(IFNULL(volume_year,           '${purr_null}', CAST(volume_year AS VARCHAR)),           '${purr_delimiter}') AS m_volume_year,
      LIST(IFNULL(water_volume,          '${purr_null}', CAST(water_volume AS VARCHAR)),          '${purr_delimiter}') AS m_water_volume
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

const asset_id_keys = ["w_uwi"];

const default_chunk = 100; // 1000

const notes = [
  "Yes, this will be quite slow.",
  "Many fields in GX_PDEN_VOL_SUM_BY_MONTH omitted to conserve memory.",
];

const order = `ORDER BY w_uwi`;

const prefixes = {
  w_: "well",
  m_: "gx_pden_vol_sum_by_month",
  p_: "well_cumulative_production",
  r_: "r_source",
};

const well_id_keys = ["w_uwi"];

///////////////////////////////////////////////////////////////////////////////

export const getAssetDNA = () => {
  return {
    asset_id_keys,
    default_chunk,
    notes,
    order,
    purr_delimiter,
    purr_null,
    purr_recent,
    prefixes,
    select,
    well_id_keys,
    xforms
  };
};
