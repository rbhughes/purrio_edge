const purr_recent = "__purrRECENT__"
const purr_null = "__purrNULL__"
const purr_delimiter = "__purrDELIMITER__";

const select = `SELECT * FROM (
  WITH w AS (
    SELECT
      uwi                        AS w_uwi
    FROM well
  ),
  t AS (
    SELECT
      uwi                        AS id_t_uwi,
      source                     AS id_t_source,
      test_type                  AS id_t_test_type,
      run_number                 AS id_t_run_number,
      MAX(row_changed_date)      AS max_row_changed_date,
      LIST(IFNULL(base_depth,                 '${purr_null}', CAST(base_depth AS VARCHAR)),               '${purr_delimiter}' ORDER BY test_number)  AS t_base_depth,
      LIST(IFNULL(base_form,                  '${purr_null}', CAST(base_form AS VARCHAR)),                '${purr_delimiter}' ORDER BY test_number)  AS t_base_form,
      LIST(IFNULL(bottom_choke_desc,          '${purr_null}', CAST(bottom_choke_desc AS VARCHAR)),        '${purr_delimiter}' ORDER BY test_number)  AS t_bottom_choke_desc,
      LIST(IFNULL(casing_press,               '${purr_null}', CAST(casing_press AS VARCHAR)),             '${purr_delimiter}' ORDER BY test_number)  AS t_casing_press,
      LIST(IFNULL(choke_size_desc,            '${purr_null}', CAST(choke_size_desc AS VARCHAR)),          '${purr_delimiter}' ORDER BY test_number)  AS t_choke_size_desc,
      LIST(IFNULL(flow_press,                 '${purr_null}', CAST(flow_press AS VARCHAR)),               '${purr_delimiter}' ORDER BY test_number)  AS t_flow_press,
      LIST(IFNULL(flow_temp,                  '${purr_null}', CAST(flow_temp AS VARCHAR)),                '${purr_delimiter}' ORDER BY test_number)  AS t_flow_temp,
      LIST(IFNULL(gas_flow_amt,               '${purr_null}', CAST(gas_flow_amt AS VARCHAR)),             '${purr_delimiter}' ORDER BY test_number)  AS t_gas_flow_amt,
      LIST(IFNULL(gas_flow_amt_uom,           '${purr_null}', CAST(gas_flow_amt_uom AS VARCHAR)),         '${purr_delimiter}' ORDER BY test_number)  AS t_gas_flow_amt_uom,
      LIST(IFNULL(gor,                        '${purr_null}', CAST(gor AS VARCHAR)),                      '${purr_delimiter}' ORDER BY test_number)  AS t_gor,
      LIST(IFNULL(gx_base_form_alias,         '${purr_null}', CAST(gx_base_form_alias AS VARCHAR)),       '${purr_delimiter}' ORDER BY test_number)  AS t_gx_base_form_alias,
      LIST(IFNULL(gx_bhp_z,                   '${purr_null}', CAST(gx_bhp_z AS VARCHAR)),                 '${purr_delimiter}' ORDER BY test_number)  AS t_gx_bhp_z,
      LIST(IFNULL(gx_co2_pct,                 '${purr_null}', CAST(gx_co2_pct AS VARCHAR)),               '${purr_delimiter}' ORDER BY test_number)  AS t_gx_co2_pct,
      LIST(IFNULL(gx_conversion_factor,       '${purr_null}', CAST(gx_conversion_factor AS VARCHAR)),     '${purr_delimiter}' ORDER BY test_number)  AS t_gx_conversion_factor,
      LIST(IFNULL(gx_cushion_type,            '${purr_null}', CAST(gx_cushion_type AS VARCHAR)),          '${purr_delimiter}' ORDER BY test_number)  AS t_gx_cushion_type,
      LIST(IFNULL(gx_cushion_vol,             '${purr_null}', CAST(gx_cushion_vol AS VARCHAR)),           '${purr_delimiter}' ORDER BY test_number)  AS t_gx_cushion_vol,
      LIST(IFNULL(gx_cushion_vol_ouom,        '${purr_null}', CAST(gx_cushion_vol_ouom AS VARCHAR)),      '${purr_delimiter}' ORDER BY test_number)  AS t_gx_cushion_vol_ouom,
      LIST(IFNULL(gx_ft_flow_amt,             '${purr_null}', CAST(gx_ft_flow_amt AS VARCHAR)),           '${purr_delimiter}' ORDER BY test_number)  AS t_gx_ft_flow_amt,
      LIST(IFNULL(gx_ft_flow_amt_uom,         '${purr_null}', CAST(gx_ft_flow_amt_uom AS VARCHAR)),       '${purr_delimiter}' ORDER BY test_number)  AS t_gx_ft_flow_amt_uom,
      LIST(IFNULL(gx_gas_cum,                 '${purr_null}', CAST(gx_gas_cum AS VARCHAR)),               '${purr_delimiter}' ORDER BY test_number)  AS t_gx_gas_cum,
      LIST(IFNULL(gx_max_ft_flow_rate,        '${purr_null}', CAST(gx_max_ft_flow_rate AS VARCHAR)),      '${purr_delimiter}' ORDER BY test_number)  AS t_gx_max_ft_flow_rate,
      LIST(IFNULL(gx_max_ft_flow_rate_uom,    '${purr_null}', CAST(gx_max_ft_flow_rate_uom AS VARCHAR)),  '${purr_delimiter}' ORDER BY test_number)  AS t_gx_max_ft_flow_rate_uom,
      LIST(IFNULL(gx_recorder_depth,          '${purr_null}', CAST(gx_recorder_depth AS VARCHAR)),        '${purr_delimiter}' ORDER BY test_number)  AS t_gx_recorder_depth,
      LIST(IFNULL(gx_top_form_alias,          '${purr_null}', CAST(gx_top_form_alias AS VARCHAR)),        '${purr_delimiter}' ORDER BY test_number)  AS t_gx_top_form_alias,
      LIST(IFNULL(gx_tracer,                  '${purr_null}', CAST(gx_tracer AS VARCHAR)),                '${purr_delimiter}' ORDER BY test_number)  AS t_gx_tracer,
      LIST(IFNULL(gx_z_factor,                '${purr_null}', CAST(gx_z_factor AS VARCHAR)),              '${purr_delimiter}' ORDER BY test_number)  AS t_gx_z_factor,
      LIST(IFNULL(h2s_pct,                    '${purr_null}', CAST(h2s_pct AS VARCHAR)),                  '${purr_delimiter}' ORDER BY test_number)  AS t_h2s_pct,
      LIST(IFNULL(max_gas_flow_rate,          '${purr_null}', CAST(max_gas_flow_rate AS VARCHAR)),        '${purr_delimiter}' ORDER BY test_number)  AS t_max_gas_flow_rate,
      LIST(IFNULL(max_hydrostatic_press,      '${purr_null}', CAST(max_hydrostatic_press AS VARCHAR)),    '${purr_delimiter}' ORDER BY test_number)  AS t_max_hydrostatic_press,
      LIST(IFNULL(max_oil_flow_rate,          '${purr_null}', CAST(max_oil_flow_rate AS VARCHAR)),        '${purr_delimiter}' ORDER BY test_number)  AS t_max_oil_flow_rate,
      LIST(IFNULL(max_water_flow_rate,        '${purr_null}', CAST(max_water_flow_rate AS VARCHAR)),      '${purr_delimiter}' ORDER BY test_number)  AS t_max_water_flow_rate,
      LIST(IFNULL(oil_flow_amt,               '${purr_null}', CAST(oil_flow_amt AS VARCHAR)),             '${purr_delimiter}' ORDER BY test_number)  AS t_oil_flow_amt,
      LIST(IFNULL(oil_flow_amt_uom,           '${purr_null}', CAST(oil_flow_amt_uom AS VARCHAR)),         '${purr_delimiter}' ORDER BY test_number)  AS t_oil_flow_amt_uom,
      LIST(IFNULL(oil_gravity,                '${purr_null}', CAST(oil_gravity AS VARCHAR)),              '${purr_delimiter}' ORDER BY test_number)  AS t_oil_gravity,
      LIST(IFNULL(primary_fluid_recovered,    '${purr_null}', CAST(primary_fluid_recovered AS VARCHAR)),  '${purr_delimiter}' ORDER BY test_number)  AS t_primary_fluid_recovered,
      LIST(IFNULL(production_method,          '${purr_null}', CAST(production_method AS VARCHAR)),        '${purr_delimiter}' ORDER BY test_number)  AS t_production_method,
      LIST(IFNULL(remark,                     '${purr_null}', CAST(remark AS VARCHAR)),                   '${purr_delimiter}' ORDER BY test_number)  AS t_remark,
      LIST(IFNULL(report_temp,                '${purr_null}', CAST(report_temp AS VARCHAR)),              '${purr_delimiter}' ORDER BY test_number)  AS t_report_temp,
      LIST(IFNULL(row_changed_date,           '${purr_null}', CAST(row_changed_date AS VARCHAR)),         '${purr_delimiter}' ORDER BY test_number)  AS t_row_changed_date,
      LIST(IFNULL(shut_off_type,              '${purr_null}', CAST(shut_off_type AS VARCHAR)),            '${purr_delimiter}' ORDER BY test_number)  AS t_shut_off_type,
      LIST(IFNULL(test_date,                  '${purr_null}', CAST(test_date AS VARCHAR)),                '${purr_delimiter}' ORDER BY test_number)  AS t_test_date,
      LIST(IFNULL(test_duration,              '${purr_null}', CAST(test_duration AS VARCHAR)),            '${purr_delimiter}' ORDER BY test_number)  AS t_test_duration,
      LIST(IFNULL(test_number,                '${purr_null}', CAST(test_number AS VARCHAR)),              '${purr_delimiter}' ORDER BY test_number)  AS t_test_number,
      LIST(IFNULL(test_subtype,               '${purr_null}', CAST(test_subtype AS VARCHAR)),             '${purr_delimiter}' ORDER BY test_number)  AS t_test_subtype,
      LIST(IFNULL(top_choke_desc,             '${purr_null}', CAST(top_choke_desc AS VARCHAR)),           '${purr_delimiter}' ORDER BY test_number)  AS t_top_choke_desc,
      LIST(IFNULL(top_depth,                  '${purr_null}', CAST(top_depth AS VARCHAR)),                '${purr_delimiter}' ORDER BY test_number)  AS t_top_depth,
      LIST(IFNULL(top_form,                   '${purr_null}', CAST(top_form AS VARCHAR)),                 '${purr_delimiter}' ORDER BY test_number)  AS t_top_form,
      LIST(IFNULL(water_flow_amt,             '${purr_null}', CAST(water_flow_amt AS VARCHAR)),           '${purr_delimiter}' ORDER BY test_number)  AS t_water_flow_amt,
      LIST(IFNULL(water_flow_amt_uom,         '${purr_null}', CAST(water_flow_amt_uom AS VARCHAR)),       '${purr_delimiter}' ORDER BY test_number)  AS t_water_flow_amt_uom
    FROM well_test
    WHERE test_type = 'DST'
    ${purr_recent}
    GROUP BY uwi, source, test_type, run_number
  ), 
  s AS (
    SELECT
      uwi                        AS id_s_uwi,
      source                     AS id_s_source,
      test_type                  AS id_s_test_type,
      run_number                 AS id_s_run_number,
      LIST(IFNULL(end_press,                  '${purr_null}', CAST(end_press AS VARCHAR)),                '${purr_delimiter}' ORDER BY test_number)  AS s_end_press,
      LIST(IFNULL(gx_duration,                '${purr_null}', CAST(gx_duration AS VARCHAR)),              '${purr_delimiter}' ORDER BY test_number)  AS s_gx_duration,
      LIST(IFNULL(period_type,                '${purr_null}', CAST(period_type AS VARCHAR)),              '${purr_delimiter}' ORDER BY test_number)  AS s_period_type,
      LIST(IFNULL(row_changed_date,           '${purr_null}', CAST(row_changed_date AS VARCHAR)),         '${purr_delimiter}' ORDER BY test_number)  AS s_row_changed_date,
      LIST(IFNULL(start_press,                '${purr_null}', CAST(start_press AS VARCHAR)),              '${purr_delimiter}' ORDER BY test_number)  AS s_start_press,
      LIST(IFNULL(test_number,                '${purr_null}', CAST(test_number AS VARCHAR)),              '${purr_delimiter}' ORDER BY test_number)  AS s_test_number
    FROM well_test_pressure
    GROUP BY uwi, source, test_type, run_number
  ),
  p AS (
    SELECT
      uwi                        AS id_p_uwi,
      source                     AS id_p_source,
      test_type                  AS id_p_test_type,
      run_number                 AS id_p_run_number,
      LIST(IFNULL(recovery_amt,               '${purr_null}', CAST(recovery_amt AS VARCHAR)),             '${purr_delimiter}' ORDER BY test_number)  AS p_recovery_amt,
      LIST(IFNULL(recovery_amt_uom,           '${purr_null}', CAST(recovery_amt_uom AS VARCHAR)),         '${purr_delimiter}' ORDER BY test_number)  AS p_recovery_amt_uom,
      LIST(IFNULL(recovery_method,            '${purr_null}', CAST(recovery_method AS VARCHAR)),          '${purr_delimiter}' ORDER BY test_number)  AS p_recovery_method,
      LIST(IFNULL(recovery_obs_no,            '${purr_null}', CAST(recovery_obs_no AS VARCHAR)),          '${purr_delimiter}' ORDER BY test_number)  AS p_recovery_obs_no,
      LIST(IFNULL(recovery_type,              '${purr_null}', CAST(recovery_type AS VARCHAR)),            '${purr_delimiter}' ORDER BY test_number)  AS p_recovery_type,
      LIST(IFNULL(remark,                     '${purr_null}', CAST(remark AS VARCHAR)),                   '${purr_delimiter}' ORDER BY test_number)  AS p_remark,
      LIST(IFNULL(row_changed_date,           '${purr_null}', CAST(row_changed_date AS VARCHAR)),         '${purr_delimiter}' ORDER BY test_number)  AS p_row_changed_date,
      LIST(IFNULL(test_number,                '${purr_null}', CAST(test_number AS VARCHAR)),              '${purr_delimiter}' ORDER BY test_number)  AS p_test_number
    FROM well_test_recovery
    WHERE recovery_method = 'PIPE'
    GROUP BY uwi, source, test_type, run_number
  ),
  f AS (
    SELECT
      uwi                        AS id_f_uwi,
      source                     AS id_f_source,
      test_type                  AS id_f_test_type,
      run_number                 AS id_f_run_number,
      LIST(IFNULL(fluid_type,                '${purr_null}', CAST(fluid_type AS VARCHAR)),                '${purr_delimiter}' ORDER BY test_number)  AS f_fluid_type,
      LIST(IFNULL(max_fluid_rate,            '${purr_null}', CAST(max_fluid_rate AS VARCHAR)),            '${purr_delimiter}' ORDER BY test_number)  AS f_max_fluid_rate,
      LIST(IFNULL(max_fluid_rate_uom,        '${purr_null}', CAST(max_fluid_rate_uom AS VARCHAR)),        '${purr_delimiter}' ORDER BY test_number)  AS f_max_fluid_rate_uom,
      LIST(IFNULL(row_changed_date,          '${purr_null}', CAST(row_changed_date AS VARCHAR)),          '${purr_delimiter}' ORDER BY test_number)  AS f_row_changed_date,
      LIST(IFNULL(test_number,               '${purr_null}', CAST(test_number AS VARCHAR)),               '${purr_delimiter}' ORDER BY test_number)  AS f_test_number,
      LIST(IFNULL(tts_elasped_time,          '${purr_null}', CAST(tts_elasped_time AS VARCHAR)),          '${purr_delimiter}' ORDER BY test_number)  AS f_tts_elasped_time  --sic
    FROM well_test_flow
    GROUP BY uwi, source, test_type, run_number
  )
  SELECT
    w.*,
    t.*,
    s.*,
    p.*,
    f.*
  FROM w
  JOIN t ON w.w_uwi = t.id_t_uwi
  LEFT OUTER JOIN s 
    ON s.id_s_uwi = t.id_t_uwi
    AND s.id_s_source = t.id_t_source
    AND s.id_s_test_type = t.id_t_test_type
    AND s.id_s_run_number = t.id_t_run_number
  LEFT OUTER JOIN p
    ON p.id_p_uwi = t.id_t_uwi
    AND p.id_p_source = t.id_t_source
    AND p.id_p_test_type = t.id_t_test_type
    AND p.id_p_run_number = t.id_t_run_number
  LEFT OUTER JOIN f
    ON f.id_f_uwi = t.id_t_uwi
    AND f.id_f_source = t.id_t_source
    AND f.id_f_test_type = t.id_t_test_type
    AND f.id_f_run_number = t.id_t_run_number
) x`;

const xforms = {
  // WELL

  w_uwi: {
    ts_type: "string",
  },

  // WELL_TEST

  id_t_uwi: {
    ts_type: "string",
  },
  id_t_source: {
    ts_type: "string",
  },
  id_t_test_type: {
    ts_type: "string",
  },
  id_t_run_number: {
    ts_type: "string",
  },
  max_row_changed_date: {
    ts_type: "date",
  },

  t_base_depth: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_base_form: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_bottom_choke_desc: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_casing_press: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_choke_size_desc: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_flow_press: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_flow_temp: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_gas_flow_amt: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_gas_flow_amt_uom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_gor: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_gx_base_form_alias: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_gx_bhp_z: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_gx_co2_pct: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_gx_conversion_factor: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_gx_cushion_type: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_gx_cushion_vol: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_gx_cushion_vol_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_gx_ft_flow_amt: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_gx_ft_flow_amt_uom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_gx_gas_cum: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_gx_max_ft_flow_rate: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_gx_max_ft_flow_rate_uom: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_gx_recorder_depth: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_gx_top_form_alias: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_gx_tracer: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_gx_z_factor: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_h2s_pct: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_max_gas_flow_rate: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_max_hydrostatic_press: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_max_oil_flow_rate: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_max_water_flow_rate: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_oil_flow_amt: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_oil_flow_amt_uom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_oil_gravity: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_primary_fluid_recovered: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_production_method: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_remark: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_report_temp: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_row_changed_date: {
    ts_type: "date",
    xform: "delimited_array_with_nulls",
  },
  t_run_number: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_shut_off_type: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_source: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_test_date: {
    ts_type: "date",
    xform: "delimited_array_with_nulls",
  },
  t_test_duration: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_test_number: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_test_subtype: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_test_type: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_top_choke_desc: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_top_depth: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_top_form: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_uwi: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_water_flow_amt: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_water_flow_amt_uom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },

  // WELL_TEST_PRESSURE

  id_s_uwi: {
    ts_type: "string",
  },
  id_s_source: {
    ts_type: "string",
  },
  id_s_test_type: {
    ts_type: "string",
  },
  id_s_run_number: {
    ts_type: "string",
  },
  s_end_press: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  // s_end_press_ouom: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  s_gx_duration: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  s_period_type: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  // s_quality_code: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // s_remark: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  s_row_changed_date: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  // s_run_number: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // s_source: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  s_start_press: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  // s_start_press_ouom: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  s_test_number: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  // s_test_type: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // s_uwi: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },

  // WELL_TEST_RECOVERY (PIPE)

  id_p_uwi: {
    ts_type: "string",
  },
  id_p_source: {
    ts_type: "string",
  },
  id_p_test_type: {
    ts_type: "string",
  },
  id_p_run_number: {
    ts_type: "string",
  },
  // p_period_type: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  p_recovery_amt: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  // p_recovery_amt_ouom: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  p_recovery_amt_uom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  p_recovery_method: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  p_recovery_obs_no: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  p_recovery_type: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  p_remark: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  p_row_changed_date: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  // p_run_number: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // p_source: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  p_test_number: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  // p_test_type: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // p_uwi: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },

  // // WELL_TEST_RECOVERY (CHAMBER)

  // id_c_uwi: {
  //   ts_type: "string",
  // },
  // id_c_source: {
  //   ts_type: "string",
  // },
  // id_c_test_type: {
  //   ts_type: "string",
  // },
  // id_c_run_number: {
  //   ts_type: "string",
  // },
  // c_period_type: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // c_recovery_amt: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // c_recovery_amt_ouom: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // c_recovery_amt_uom: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // c_recovery_method: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // c_recovery_obs_no: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // c_recovery_type: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // c_remark: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // c_row_changed_date: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // c_run_number: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // c_source: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // c_test_number: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // c_test_type: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // c_uwi: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },

  // WELL_TEST_FLOW

  id_f_uwi: {
    ts_type: "string",
  },
  id_f_source: {
    ts_type: "string",
  },
  id_f_test_type: {
    ts_type: "string",
  },
  id_f_run_number: {
    ts_type: "string",
  },
  f_fluid_type: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_max_fluid_rate: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  // f_max_fluid_rate_ouom: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  f_max_fluid_rate_uom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  // f_max_surface_press: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // f_max_surface_press_ouom: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // f_period_obs_no: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // f_period_type: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // f_remark: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  f_row_changed_date: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  // f_run_number: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // f_source: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  f_test_number: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  // f_test_type: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  f_tts_elapsed_time_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_tts_elasped_time: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  // f_uwi: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },

  // WELL_TEST_ANALYSIS

  // id_a_uwi: {
  //   ts_type: "string",
  // },
  // id_a_source: {
  //   ts_type: "string",
  // },
  // id_a_test_type: {
  //   ts_type: "string",
  // },
  // id_a_run_number: {
  //   ts_type: "string",
  // },
  // a_analysis_obs_no: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_bsw: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_condensate_gravity: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_condensate_gravity_ouom: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_condensate_ratio: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_condensate_ratio_ouom: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_condensate_temp: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_condensate_temp_ouom: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_fluid_type: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_gas_content: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_gas_gravity: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_gas_gravity_ouom: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_gor: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_gor_ouom: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_gwr: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_gwr_ouom: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_gx_fluid_percent: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_h2s_pct: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_lgr: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_lgr_ouom: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_oil_density: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_oil_densty_ouom: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_oil_gravity: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_oil_gravity_ouom: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_oil_temp: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_oil_temp_ouom: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_period_obs_no: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_period_type: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_row_changed_date: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_run_number: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_salinity_type: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_source: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_sulphur_pct: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_test_number: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_test_type: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_uwi: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_water_cut: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_water_resistivity: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_water_resistivity_ouom: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_water_salinity: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_water_salinity_ouom: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_water_temp: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_water_temp_ouom: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // a_wor: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },

  // // WELL_TEST_MUD

  // id_m_uwi: {
  //   ts_type: "string",
  // },
  // id_m_source: {
  //   ts_type: "string",
  // },
  // id_m_test_type: {
  //   ts_type: "string",
  // },
  // id_m_run_number: {
  //   ts_type: "string",
  // },
  // m_filtrate_resistivity: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_filtrate_resistivity_ouom: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_filtrate_temp: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_filtrate_temp_ouom: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_gx_filter_cake_thickness: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_gx_tracer: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_gx_water_loss: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_mud_resistivity: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_mud_resistivity_ouom: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_mud_temp: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_mud_temp_ouom: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_mud_type: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_mud_viscosity: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_mud_viscosity_ouom: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_mud_weight: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_mud_weight_ouom: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_row_changed_date: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_run_number: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_source: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_test_number: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_test_type: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // m_uwi: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
};

const asset_id_keys = [
  "w_uwi",
  "id_t_source",
  "id_t_test_type",
  "id_t_run_number",
];

const default_chunk = 100; // 100

const notes = [
  "Many fields excluded to save memory: WELL_TEST_ANALYSIS, WELL_TEST_MUD, etc.",
];

const order = `ORDER BY w_uwi, id_t_source, id_t_test_type, id_t_run_number`;

const prefixes = {
  w_: "well",
  t_: "well_test",
  s_: "well_test_pressure",
  p_: "well_test_recovery",
  c_: "well_test_recovery",
  f_: "well_test_flow",
  a_: "well_test_analysis",
  m_: "well_test_mud",
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
