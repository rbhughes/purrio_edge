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
    t AS (
      SELECT
        uwi                        AS id_t_uwi,
        source                     AS id_t_source,
        test_type                  AS id_t_test_type,
        run_number                 AS id_t_run_number,
        LIST(IFNULL(base_depth,                 '${N}', CAST(base_depth AS VARCHAR)),               '${D}' ORDER BY test_number)  AS t_base_depth,
        LIST(IFNULL(base_form,                  '${N}', CAST(base_form AS VARCHAR)),                '${D}' ORDER BY test_number)  AS t_base_form,
        LIST(IFNULL(bottom_choke_desc,          '${N}', CAST(bottom_choke_desc AS VARCHAR)),        '${D}' ORDER BY test_number)  AS t_bottom_choke_desc,
        LIST(IFNULL(casing_press,               '${N}', CAST(casing_press AS VARCHAR)),             '${D}' ORDER BY test_number)  AS t_casing_press,
        LIST(IFNULL(choke_size_desc,            '${N}', CAST(choke_size_desc AS VARCHAR)),          '${D}' ORDER BY test_number)  AS t_choke_size_desc,
        LIST(IFNULL(flow_press,                 '${N}', CAST(flow_press AS VARCHAR)),               '${D}' ORDER BY test_number)  AS t_flow_press,
        LIST(IFNULL(flow_temp,                  '${N}', CAST(flow_temp AS VARCHAR)),                '${D}' ORDER BY test_number)  AS t_flow_temp,
        LIST(IFNULL(gas_flow_amt,               '${N}', CAST(gas_flow_amt AS VARCHAR)),             '${D}' ORDER BY test_number)  AS t_gas_flow_amt,
        LIST(IFNULL(gas_flow_amt_uom,           '${N}', CAST(gas_flow_amt_uom AS VARCHAR)),         '${D}' ORDER BY test_number)  AS t_gas_flow_amt_uom,
        LIST(IFNULL(gor,                        '${N}', CAST(gor AS VARCHAR)),                      '${D}' ORDER BY test_number)  AS t_gor,
        LIST(IFNULL(gx_base_form_alias,         '${N}', CAST(gx_base_form_alias AS VARCHAR)),       '${D}' ORDER BY test_number)  AS t_gx_base_form_alias,
        LIST(IFNULL(gx_bhp_z,                   '${N}', CAST(gx_bhp_z AS VARCHAR)),                 '${D}' ORDER BY test_number)  AS t_gx_bhp_z,
        LIST(IFNULL(gx_co2_pct,                 '${N}', CAST(gx_co2_pct AS VARCHAR)),               '${D}' ORDER BY test_number)  AS t_gx_co2_pct,
        LIST(IFNULL(gx_conversion_factor,       '${N}', CAST(gx_conversion_factor AS VARCHAR)),     '${D}' ORDER BY test_number)  AS t_gx_conversion_factor,
        LIST(IFNULL(gx_cushion_type,            '${N}', CAST(gx_cushion_type AS VARCHAR)),          '${D}' ORDER BY test_number)  AS t_gx_cushion_type,
        LIST(IFNULL(gx_cushion_vol,             '${N}', CAST(gx_cushion_vol AS VARCHAR)),           '${D}' ORDER BY test_number)  AS t_gx_cushion_vol,
        LIST(IFNULL(gx_cushion_vol_ouom,        '${N}', CAST(gx_cushion_vol_ouom AS VARCHAR)),      '${D}' ORDER BY test_number)  AS t_gx_cushion_vol_ouom,
        LIST(IFNULL(gx_ft_flow_amt,             '${N}', CAST(gx_ft_flow_amt AS VARCHAR)),           '${D}' ORDER BY test_number)  AS t_gx_ft_flow_amt,
        LIST(IFNULL(gx_ft_flow_amt_uom,         '${N}', CAST(gx_ft_flow_amt_uom AS VARCHAR)),       '${D}' ORDER BY test_number)  AS t_gx_ft_flow_amt_uom,
        LIST(IFNULL(gx_gas_cum,                 '${N}', CAST(gx_gas_cum AS VARCHAR)),               '${D}' ORDER BY test_number)  AS t_gx_gas_cum,
        LIST(IFNULL(gx_max_ft_flow_rate,        '${N}', CAST(gx_max_ft_flow_rate AS VARCHAR)),      '${D}' ORDER BY test_number)  AS t_gx_max_ft_flow_rate,
        LIST(IFNULL(gx_max_ft_flow_rate_uom,    '${N}', CAST(gx_max_ft_flow_rate_uom AS VARCHAR)),  '${D}' ORDER BY test_number)  AS t_gx_max_ft_flow_rate_uom,
        LIST(IFNULL(gx_recorder_depth,          '${N}', CAST(gx_recorder_depth AS VARCHAR)),        '${D}' ORDER BY test_number)  AS t_gx_recorder_depth,
        LIST(IFNULL(gx_top_form_alias,          '${N}', CAST(gx_top_form_alias AS VARCHAR)),        '${D}' ORDER BY test_number)  AS t_gx_top_form_alias,
        LIST(IFNULL(gx_tracer,                  '${N}', CAST(gx_tracer AS VARCHAR)),                '${D}' ORDER BY test_number)  AS t_gx_tracer,
        LIST(IFNULL(gx_z_factor,                '${N}', CAST(gx_z_factor AS VARCHAR)),              '${D}' ORDER BY test_number)  AS t_gx_z_factor,
        LIST(IFNULL(h2s_pct,                    '${N}', CAST(h2s_pct AS VARCHAR)),                  '${D}' ORDER BY test_number)  AS t_h2s_pct,
        LIST(IFNULL(max_gas_flow_rate,          '${N}', CAST(max_gas_flow_rate AS VARCHAR)),        '${D}' ORDER BY test_number)  AS t_max_gas_flow_rate,
        LIST(IFNULL(max_hydrostatic_press,      '${N}', CAST(max_hydrostatic_press AS VARCHAR)),    '${D}' ORDER BY test_number)  AS t_max_hydrostatic_press,
        LIST(IFNULL(max_oil_flow_rate,          '${N}', CAST(max_oil_flow_rate AS VARCHAR)),        '${D}' ORDER BY test_number)  AS t_max_oil_flow_rate,
        LIST(IFNULL(max_water_flow_rate,        '${N}', CAST(max_water_flow_rate AS VARCHAR)),      '${D}' ORDER BY test_number)  AS t_max_water_flow_rate,
        LIST(IFNULL(oil_flow_amt,               '${N}', CAST(oil_flow_amt AS VARCHAR)),             '${D}' ORDER BY test_number)  AS t_oil_flow_amt,
        LIST(IFNULL(oil_flow_amt_uom,           '${N}', CAST(oil_flow_amt_uom AS VARCHAR)),         '${D}' ORDER BY test_number)  AS t_oil_flow_amt_uom,
        LIST(IFNULL(oil_gravity,                '${N}', CAST(oil_gravity AS VARCHAR)),              '${D}' ORDER BY test_number)  AS t_oil_gravity,
        LIST(IFNULL(primary_fluid_recovered,    '${N}', CAST(primary_fluid_recovered AS VARCHAR)),  '${D}' ORDER BY test_number)  AS t_primary_fluid_recovered,
        LIST(IFNULL(production_method,          '${N}', CAST(production_method AS VARCHAR)),        '${D}' ORDER BY test_number)  AS t_production_method,
        LIST(IFNULL(remark,                     '${N}', CAST(remark AS VARCHAR)),                   '${D}' ORDER BY test_number)  AS t_remark,
        LIST(IFNULL(report_temp,                '${N}', CAST(report_temp AS VARCHAR)),              '${D}' ORDER BY test_number)  AS t_report_temp,
        LIST(IFNULL(row_changed_date,           '${N}', CAST(row_changed_date AS VARCHAR)),         '${D}' ORDER BY test_number)  AS t_row_changed_date,
        LIST(IFNULL(run_number,                 '${N}', CAST(run_number AS VARCHAR)),               '${D}' ORDER BY test_number)  AS t_run_number,
        LIST(IFNULL(shut_off_type,              '${N}', CAST(shut_off_type AS VARCHAR)),            '${D}' ORDER BY test_number)  AS t_shut_off_type,
        LIST(IFNULL(source,                     '${N}', CAST(source AS VARCHAR)),                   '${D}' ORDER BY test_number)  AS t_source,
        LIST(IFNULL(test_date,                  '${N}', CAST(test_date AS VARCHAR)),                '${D}' ORDER BY test_number)  AS t_test_date,
        LIST(IFNULL(test_duration,              '${N}', CAST(test_duration AS VARCHAR)),            '${D}' ORDER BY test_number)  AS t_test_duration,
        LIST(IFNULL(test_number,                '${N}', CAST(test_number AS VARCHAR)),              '${D}' ORDER BY test_number)  AS t_test_number,
        LIST(IFNULL(test_subtype,               '${N}', CAST(test_subtype AS VARCHAR)),             '${D}' ORDER BY test_number)  AS t_test_subtype,
        LIST(IFNULL(test_type,                  '${N}', CAST(test_type AS VARCHAR)),                '${D}' ORDER BY test_number)  AS t_test_type,
        LIST(IFNULL(top_choke_desc,             '${N}', CAST(top_choke_desc AS VARCHAR)),           '${D}' ORDER BY test_number)  AS t_top_choke_desc,
        LIST(IFNULL(top_depth,                  '${N}', CAST(top_depth AS VARCHAR)),                '${D}' ORDER BY test_number)  AS t_top_depth,
        LIST(IFNULL(top_form,                   '${N}', CAST(top_form AS VARCHAR)),                 '${D}' ORDER BY test_number)  AS t_top_form,
        LIST(IFNULL(uwi,                        '${N}', CAST(uwi AS VARCHAR)),                      '${D}' ORDER BY test_number)  AS t_uwi,
        LIST(IFNULL(water_flow_amt,             '${N}', CAST(water_flow_amt AS VARCHAR)),           '${D}' ORDER BY test_number)  AS t_water_flow_amt,
        LIST(IFNULL(water_flow_amt_uom,         '${N}', CAST(water_flow_amt_uom AS VARCHAR)),       '${D}' ORDER BY test_number)  AS t_water_flow_amt_uom
      FROM well_test
      WHERE test_type = 'DST'
      GROUP BY uwi, source, test_type, run_number
    ), 
    s AS (
      SELECT
        uwi                        AS id_s_uwi,
        source                     AS id_s_source,
        test_type                  AS id_s_test_type,
        run_number                 AS id_s_run_number,
        LIST(IFNULL(end_press,                  '${N}', CAST(end_press AS VARCHAR)),                '${D}' ORDER BY test_number)  AS s_end_press,
        LIST(IFNULL(end_press_ouom,             '${N}', CAST(end_press_ouom AS VARCHAR)),           '${D}' ORDER BY test_number)  AS s_end_press_ouom,
        LIST(IFNULL(gx_duration,                '${N}', CAST(gx_duration AS VARCHAR)),              '${D}' ORDER BY test_number)  AS s_gx_duration,
        LIST(IFNULL(period_type,                '${N}', CAST(period_type AS VARCHAR)),              '${D}' ORDER BY test_number)  AS s_period_type,
        LIST(IFNULL(quality_code,               '${N}', CAST(quality_code AS VARCHAR)),             '${D}' ORDER BY test_number)  AS s_quality_code,
        LIST(IFNULL(remark,                     '${N}', CAST(remark AS VARCHAR)),                   '${D}' ORDER BY test_number)  AS s_remark,
        LIST(IFNULL(row_changed_date,           '${N}', CAST(row_changed_date AS VARCHAR)),         '${D}' ORDER BY test_number)  AS s_row_changed_date,
        LIST(IFNULL(run_number,                 '${N}', CAST(run_number AS VARCHAR)),               '${D}' ORDER BY test_number)  AS s_run_number,
        LIST(IFNULL(source,                     '${N}', CAST(source AS VARCHAR)),                   '${D}' ORDER BY test_number)  AS s_source,
        LIST(IFNULL(start_press,                '${N}', CAST(start_press AS VARCHAR)),              '${D}' ORDER BY test_number)  AS s_start_press,
        LIST(IFNULL(start_press_ouom,           '${N}', CAST(start_press_ouom AS VARCHAR)),         '${D}' ORDER BY test_number)  AS s_start_press_ouom,
        LIST(IFNULL(test_number,                '${N}', CAST(test_number AS VARCHAR)),              '${D}' ORDER BY test_number)  AS s_test_number,
        LIST(IFNULL(test_type,                  '${N}', CAST(test_type AS VARCHAR)),                '${D}' ORDER BY test_number)  AS s_test_type,
        LIST(IFNULL(uwi,                        '${N}', CAST(uwi AS VARCHAR)),                      '${D}' ORDER BY test_number)  AS s_uwi
      FROM well_test_pressure
      GROUP BY uwi, source, test_type, run_number
    ),
    p AS (
      SELECT
        uwi                        AS id_p_uwi,
        source                     AS id_p_source,
        test_type                  AS id_p_test_type,
        run_number                 AS id_p_run_number,
        LIST(IFNULL(period_type,                '${N}', CAST(period_type AS VARCHAR)),              '${D}' ORDER BY test_number)  AS p_period_type,
        LIST(IFNULL(recovery_amt,               '${N}', CAST(recovery_amt AS VARCHAR)),             '${D}' ORDER BY test_number)  AS p_recovery_amt,
        LIST(IFNULL(recovery_amt_ouom,          '${N}', CAST(recovery_amt_ouom AS VARCHAR)),        '${D}' ORDER BY test_number)  AS p_recovery_amt_ouom,
        LIST(IFNULL(recovery_amt_uom,           '${N}', CAST(recovery_amt_uom AS VARCHAR)),         '${D}' ORDER BY test_number)  AS p_recovery_amt_uom,
        LIST(IFNULL(recovery_method,            '${N}', CAST(recovery_method AS VARCHAR)),          '${D}' ORDER BY test_number)  AS p_recovery_method,
        LIST(IFNULL(recovery_obs_no,            '${N}', CAST(recovery_obs_no AS VARCHAR)),          '${D}' ORDER BY test_number)  AS p_recovery_obs_no,
        LIST(IFNULL(recovery_type,              '${N}', CAST(recovery_type AS VARCHAR)),            '${D}' ORDER BY test_number)  AS p_recovery_type,
        LIST(IFNULL(remark,                     '${N}', CAST(remark AS VARCHAR)),                   '${D}' ORDER BY test_number)  AS p_remark,
        LIST(IFNULL(row_changed_date,           '${N}', CAST(row_changed_date AS VARCHAR)),         '${D}' ORDER BY test_number)  AS p_row_changed_date,
        LIST(IFNULL(run_number,                 '${N}', CAST(run_number AS VARCHAR)),               '${D}' ORDER BY test_number)  AS p_run_number,
        LIST(IFNULL(source,                     '${N}', CAST(source AS VARCHAR)),                   '${D}' ORDER BY test_number)  AS p_source,
        LIST(IFNULL(test_number,                '${N}', CAST(test_number AS VARCHAR)),              '${D}' ORDER BY test_number)  AS p_test_number,
        LIST(IFNULL(test_type,                  '${N}', CAST(test_type AS VARCHAR)),                '${D}' ORDER BY test_number)  AS p_test_type,
        LIST(IFNULL(uwi,                        '${N}', CAST(uwi AS VARCHAR)),                      '${D}' ORDER BY test_number)  AS p_uwi
      FROM well_test_recovery
      WHERE recovery_method = 'PIPE'
      GROUP BY uwi, source, test_type, run_number
    ),
    c AS (
      SELECT
        uwi                        AS id_c_uwi,
        source                     AS id_c_source,
        test_type                  AS id_c_test_type,
        run_number                 AS id_c_run_number,
        LIST(IFNULL(period_type,                '${N}', CAST(period_type AS VARCHAR)),              '${D}' ORDER BY test_number)  AS c_period_type,
        LIST(IFNULL(recovery_amt,               '${N}', CAST(recovery_amt AS VARCHAR)),             '${D}' ORDER BY test_number)  AS c_recovery_amt,
        LIST(IFNULL(recovery_amt_ouom,          '${N}', CAST(recovery_amt_ouom AS VARCHAR)),        '${D}' ORDER BY test_number)  AS c_recovery_amt_ouom,
        LIST(IFNULL(recovery_amt_uom,           '${N}', CAST(recovery_amt_uom AS VARCHAR)),         '${D}' ORDER BY test_number)  AS c_recovery_amt_uom,
        LIST(IFNULL(recovery_method,            '${N}', CAST(recovery_method AS VARCHAR)),          '${D}' ORDER BY test_number)  AS c_recovery_method,
        LIST(IFNULL(recovery_obs_no,            '${N}', CAST(recovery_obs_no AS VARCHAR)),          '${D}' ORDER BY test_number)  AS c_recovery_obs_no,
        LIST(IFNULL(recovery_type,              '${N}', CAST(recovery_type AS VARCHAR)),            '${D}' ORDER BY test_number)  AS c_recovery_type,
        LIST(IFNULL(remark,                     '${N}', CAST(remark AS VARCHAR)),                   '${D}' ORDER BY test_number)  AS c_remark,
        LIST(IFNULL(row_changed_date,           '${N}', CAST(row_changed_date AS VARCHAR)),         '${D}' ORDER BY test_number)  AS c_row_changed_date,
        LIST(IFNULL(run_number,                 '${N}', CAST(run_number AS VARCHAR)),               '${D}' ORDER BY test_number)  AS c_run_number,
        LIST(IFNULL(source,                     '${N}', CAST(source AS VARCHAR)),                   '${D}' ORDER BY test_number)  AS c_source,
        LIST(IFNULL(test_number,                '${N}', CAST(test_number AS VARCHAR)),              '${D}' ORDER BY test_number)  AS c_test_number,
        LIST(IFNULL(test_type,                  '${N}', CAST(test_type AS VARCHAR)),                '${D}' ORDER BY test_number)  AS c_test_type,
        LIST(IFNULL(uwi,                        '${N}', CAST(uwi AS VARCHAR)),                      '${D}' ORDER BY test_number)  AS c_uwi
      FROM well_test_recovery
      WHERE recovery_method = 'CHAMBER'
      GROUP BY uwi, source, test_type, run_number
    ),
    f AS (
      SELECT
        uwi                        AS id_f_uwi,
        source                     AS id_f_source,
        test_type                  AS id_f_test_type,
        run_number                 AS id_f_run_number,
        LIST(IFNULL(fluid_type,                '${N}', CAST(fluid_type AS VARCHAR)),                '${D}' ORDER BY test_number)  AS f_fluid_type,
        LIST(IFNULL(max_fluid_rate,            '${N}', CAST(max_fluid_rate AS VARCHAR)),            '${D}' ORDER BY test_number)  AS f_max_fluid_rate,
        LIST(IFNULL(max_fluid_rate_ouom,       '${N}', CAST(max_fluid_rate_ouom  AS VARCHAR)),      '${D}' ORDER BY test_number)  AS f_max_fluid_rate_ouom,
        LIST(IFNULL(max_fluid_rate_uom,        '${N}', CAST(max_fluid_rate_uom AS VARCHAR)),        '${D}' ORDER BY test_number)  AS f_max_fluid_rate_uom,
        LIST(IFNULL(max_surface_press,         '${N}', CAST(max_surface_press AS VARCHAR)),         '${D}' ORDER BY test_number)  AS f_max_surface_press,
        LIST(IFNULL(max_surface_press_ouom,    '${N}', CAST(max_surface_press_ouom AS VARCHAR)),    '${D}' ORDER BY test_number)  AS f_max_surface_press_ouom,
        LIST(IFNULL(period_obs_no,             '${N}', CAST(period_obs_no AS VARCHAR)),             '${D}' ORDER BY test_number)  AS f_period_obs_no,
        LIST(IFNULL(period_type,               '${N}', CAST(period_type AS VARCHAR)),               '${D}' ORDER BY test_number)  AS f_period_type,
        LIST(IFNULL(remark,                    '${N}', CAST(remark AS VARCHAR)),                    '${D}' ORDER BY test_number)  AS f_remark,
        LIST(IFNULL(row_changed_date,          '${N}', CAST(row_changed_date AS VARCHAR)),          '${D}' ORDER BY test_number)  AS f_row_changed_date,
        LIST(IFNULL(run_number,                '${N}', CAST(run_number AS VARCHAR)),                '${D}' ORDER BY test_number)  AS f_run_number,
        LIST(IFNULL(source,                    '${N}', CAST(source AS VARCHAR)),                    '${D}' ORDER BY test_number)  AS f_source,
        LIST(IFNULL(test_number,               '${N}', CAST(test_number AS VARCHAR)),               '${D}' ORDER BY test_number)  AS f_test_number,
        LIST(IFNULL(test_type,                 '${N}', CAST(test_type AS VARCHAR)),                 '${D}' ORDER BY test_number)  AS f_test_type,
        LIST(IFNULL(tts_elapsed_time_ouom,     '${N}', CAST(tts_elapsed_time_ouom AS VARCHAR)),     '${D}' ORDER BY test_number)  AS f_tts_elapsed_time_ouom,
        LIST(IFNULL(tts_elasped_time,          '${N}', CAST(tts_elasped_time AS VARCHAR)),          '${D}' ORDER BY test_number)  AS f_tts_elasped_time,  --sic
        LIST(IFNULL(uwi,                       '${N}', CAST(uwi AS VARCHAR)),                       '${D}' ORDER BY test_number)  AS f_uwi
      FROM well_test_flow
      GROUP BY uwi, source, test_type, run_number
    ),
    a AS (
      SELECT
        uwi                        AS id_a_uwi,
        source                     AS id_a_source,
        test_type                  AS id_a_test_type,
        run_number                 AS id_a_run_number,
        LIST(IFNULL(analysis_obs_no,           '${N}', CAST(analysis_obs_no AS VARCHAR)),           '${D}' ORDER BY test_number)  AS a_analysis_obs_no,
        LIST(IFNULL(bsw,                       '${N}', CAST(bsw AS VARCHAR)),                       '${D}' ORDER BY test_number)  AS a_bsw,
        LIST(IFNULL(condensate_gravity,        '${N}', CAST(condensate_gravity AS VARCHAR)),        '${D}' ORDER BY test_number)  AS a_condensate_gravity,
        LIST(IFNULL(condensate_gravity_ouom,   '${N}', CAST(condensate_gravity_ouom AS VARCHAR)),   '${D}' ORDER BY test_number)  AS a_condensate_gravity_ouom,
        LIST(IFNULL(condensate_ratio,          '${N}', CAST(condensate_ratio AS VARCHAR)),          '${D}' ORDER BY test_number)  AS a_condensate_ratio,
        LIST(IFNULL(condensate_ratio_ouom,     '${N}', CAST(condensate_ratio_ouom AS VARCHAR)),     '${D}' ORDER BY test_number)  AS a_condensate_ratio_ouom,
        LIST(IFNULL(condensate_temp,           '${N}', CAST(condensate_temp AS VARCHAR)),           '${D}' ORDER BY test_number)  AS a_condensate_temp,
        LIST(IFNULL(condensate_temp_ouom,      '${N}', CAST(condensate_temp_ouom AS VARCHAR)),      '${D}' ORDER BY test_number)  AS a_condensate_temp_ouom,
        LIST(IFNULL(fluid_type,                '${N}', CAST(fluid_type AS VARCHAR)),                '${D}' ORDER BY test_number)  AS a_fluid_type,
        LIST(IFNULL(gas_content,               '${N}', CAST(gas_content AS VARCHAR)),               '${D}' ORDER BY test_number)  AS a_gas_content,
        LIST(IFNULL(gas_gravity,               '${N}', CAST(gas_gravity AS VARCHAR)),               '${D}' ORDER BY test_number)  AS a_gas_gravity,
        LIST(IFNULL(gas_gravity_ouom,          '${N}', CAST(gas_gravity_ouom AS VARCHAR)),          '${D}' ORDER BY test_number)  AS a_gas_gravity_ouom,
        LIST(IFNULL(gor,                       '${N}', CAST(gor AS VARCHAR)),                       '${D}' ORDER BY test_number)  AS a_gor,
        LIST(IFNULL(gor_ouom,                  '${N}', CAST(gor_ouom AS VARCHAR)),                  '${D}' ORDER BY test_number)  AS a_gor_ouom,
        LIST(IFNULL(gwr,                       '${N}', CAST(gwr AS VARCHAR)),                       '${D}' ORDER BY test_number)  AS a_gwr,
        LIST(IFNULL(gwr_ouom,                  '${N}', CAST(gwr_ouom AS VARCHAR)),                  '${D}' ORDER BY test_number)  AS a_gwr_ouom,
        LIST(IFNULL(gx_fluid_percent,          '${N}', CAST(gx_fluid_percent AS VARCHAR)),          '${D}' ORDER BY test_number)  AS a_gx_fluid_percent,
        LIST(IFNULL(h2s_pct,                   '${N}', CAST(h2s_pct AS VARCHAR)),                   '${D}' ORDER BY test_number)  AS a_h2s_pct,
        LIST(IFNULL(lgr,                       '${N}', CAST(lgr AS VARCHAR)),                       '${D}' ORDER BY test_number)  AS a_lgr,
        LIST(IFNULL(lgr_ouom,                  '${N}', CAST(lgr_ouom AS VARCHAR)),                  '${D}' ORDER BY test_number)  AS a_lgr_ouom,
        LIST(IFNULL(oil_density,               '${N}', CAST(oil_density AS VARCHAR)),               '${D}' ORDER BY test_number)  AS a_oil_density,
        LIST(IFNULL(oil_densty_ouom,           '${N}', CAST(oil_densty_ouom AS VARCHAR)),           '${D}' ORDER BY test_number)  AS a_oil_densty_ouom, --sic
        LIST(IFNULL(oil_gravity,               '${N}', CAST(oil_gravity AS VARCHAR)),               '${D}' ORDER BY test_number)  AS a_oil_gravity,
        LIST(IFNULL(oil_gravity_ouom,          '${N}', CAST(oil_gravity_ouom AS VARCHAR)),          '${D}' ORDER BY test_number)  AS a_oil_gravity_ouom,
        LIST(IFNULL(oil_temp,                  '${N}', CAST(oil_temp AS VARCHAR)),                  '${D}' ORDER BY test_number)  AS a_oil_temp,
        LIST(IFNULL(oil_temp_ouom,             '${N}', CAST(oil_temp_ouom AS VARCHAR)),             '${D}' ORDER BY test_number)  AS a_oil_temp_ouom,
        LIST(IFNULL(period_obs_no,             '${N}', CAST(period_obs_no AS VARCHAR)),             '${D}' ORDER BY test_number)  AS a_period_obs_no,
        LIST(IFNULL(period_type,               '${N}', CAST(period_type AS VARCHAR)),               '${D}' ORDER BY test_number)  AS a_period_type,
        LIST(IFNULL(row_changed_date,          '${N}', CAST(row_changed_date AS VARCHAR)),          '${D}' ORDER BY test_number)  AS a_row_changed_date,
        LIST(IFNULL(run_number,                '${N}', CAST(run_number AS VARCHAR)),                '${D}' ORDER BY test_number)  AS a_run_number,
        LIST(IFNULL(salinity_type,             '${N}', CAST(salinity_type AS VARCHAR)),             '${D}' ORDER BY test_number)  AS a_salinity_type,
        LIST(IFNULL(source,                    '${N}', CAST(source AS VARCHAR)),                    '${D}' ORDER BY test_number)  AS a_source,
        LIST(IFNULL(sulphur_pct,               '${N}', CAST(sulphur_pct AS VARCHAR)),               '${D}' ORDER BY test_number)  AS a_sulphur_pct,
        LIST(IFNULL(test_number,               '${N}', CAST(test_number AS VARCHAR)),               '${D}' ORDER BY test_number)  AS a_test_number,
        LIST(IFNULL(test_type,                 '${N}', CAST(test_type AS VARCHAR)),                 '${D}' ORDER BY test_number)  AS a_test_type,
        LIST(IFNULL(uwi,                       '${N}', CAST(uwi AS VARCHAR)),                       '${D}' ORDER BY test_number)  AS a_uwi,
        LIST(IFNULL(water_cut,                 '${N}', CAST(water_cut AS VARCHAR)),                 '${D}' ORDER BY test_number)  AS a_water_cut,
        LIST(IFNULL(water_resistivity,         '${N}', CAST(water_resistivity AS VARCHAR)),         '${D}' ORDER BY test_number)  AS a_water_resistivity,
        LIST(IFNULL(water_resistivity_ouom,    '${N}', CAST(water_resistivity_ouom AS VARCHAR)),    '${D}' ORDER BY test_number)  AS a_water_resistivity_ouom,
        LIST(IFNULL(water_salinity,            '${N}', CAST(water_salinity AS VARCHAR)),            '${D}' ORDER BY test_number)  AS a_water_salinity,
        LIST(IFNULL(water_salinity_ouom,       '${N}', CAST(water_salinity_ouom AS VARCHAR)),       '${D}' ORDER BY test_number)  AS a_water_salinity_ouom,
        LIST(IFNULL(water_temp,                '${N}', CAST(water_temp AS VARCHAR)),                '${D}' ORDER BY test_number)  AS a_water_temp,
        LIST(IFNULL(water_temp_ouom,           '${N}', CAST(water_temp_ouom AS VARCHAR)),           '${D}' ORDER BY test_number)  AS a_water_temp_ouom,
        LIST(IFNULL(wor,                       '${N}', CAST(wor AS VARCHAR)),                       '${D}' ORDER BY test_number)  AS a_wor
      FROM well_test_analysis
      GROUP BY uwi, source, test_type, run_number
    ),
    m AS (
      SELECT
        uwi                        AS id_m_uwi,
        source                     AS id_m_source,
        test_type                  AS id_m_test_type,
        run_number                 AS id_m_run_number,
        LIST(IFNULL(filtrate_resistivity,      '${N}', CAST(filtrate_resistivity AS VARCHAR)),      '${D}' ORDER BY test_number)  AS m_filtrate_resistivity,
        LIST(IFNULL(filtrate_resistivity_ouom, '${N}', CAST(filtrate_resistivity_ouom AS VARCHAR)), '${D}' ORDER BY test_number)  AS m_filtrate_resistivity_ouom,
        LIST(IFNULL(filtrate_temp,             '${N}', CAST(filtrate_temp AS VARCHAR)),             '${D}' ORDER BY test_number)  AS m_filtrate_temp,
        LIST(IFNULL(filtrate_temp_ouom,        '${N}', CAST(filtrate_temp_ouom AS VARCHAR)),        '${D}' ORDER BY test_number)  AS m_filtrate_temp_ouom,
        LIST(IFNULL(gx_filter_cake_thickness,  '${N}', CAST(gx_filter_cake_thickness AS VARCHAR)),  '${D}' ORDER BY test_number)  AS m_gx_filter_cake_thickness,
        LIST(IFNULL(gx_tracer,                 '${N}', CAST(gx_tracer AS VARCHAR)),                 '${D}' ORDER BY test_number)  AS m_gx_tracer,
        LIST(IFNULL(gx_water_loss,             '${N}', CAST(gx_water_loss AS VARCHAR)),             '${D}' ORDER BY test_number)  AS m_gx_water_loss,
        LIST(IFNULL(mud_resistivity,           '${N}', CAST(mud_resistivity AS VARCHAR)),           '${D}' ORDER BY test_number)  AS m_mud_resistivity,
        LIST(IFNULL(mud_resistivity_ouom,      '${N}', CAST(mud_resistivity_ouom AS VARCHAR)),      '${D}' ORDER BY test_number)  AS m_mud_resistivity_ouom,
        LIST(IFNULL(mud_temp,                  '${N}', CAST(mud_temp AS VARCHAR)),                  '${D}' ORDER BY test_number)  AS m_mud_temp,
        LIST(IFNULL(mud_temp_ouom,             '${N}', CAST(mud_temp_ouom AS VARCHAR)),             '${D}' ORDER BY test_number)  AS m_mud_temp_ouom,
        LIST(IFNULL(mud_type,                  '${N}', CAST(mud_type AS VARCHAR)),                  '${D}' ORDER BY test_number)  AS m_mud_type,
        LIST(IFNULL(mud_viscosity,             '${N}', CAST(mud_viscosity AS VARCHAR)),             '${D}' ORDER BY test_number)  AS m_mud_viscosity,
        LIST(IFNULL(mud_viscosity_ouom,        '${N}', CAST(mud_viscosity_ouom AS VARCHAR)),        '${D}' ORDER BY test_number)  AS m_mud_viscosity_ouom,
        LIST(IFNULL(mud_weight,                '${N}', CAST(mud_weight AS VARCHAR)),                '${D}' ORDER BY test_number)  AS m_mud_weight,
        LIST(IFNULL(mud_weight_ouom,           '${N}', CAST(mud_weight_ouom AS VARCHAR)),           '${D}' ORDER BY test_number)  AS m_mud_weight_ouom,
        LIST(IFNULL(row_changed_date,          '${N}', CAST(row_changed_date AS VARCHAR)),          '${D}' ORDER BY test_number)  AS m_row_changed_date,
        LIST(IFNULL(run_number,                '${N}', CAST(run_number AS VARCHAR)),                '${D}' ORDER BY test_number)  AS m_run_number,
        LIST(IFNULL(source,                    '${N}', CAST(source AS VARCHAR)),                    '${D}' ORDER BY test_number)  AS m_source,
        LIST(IFNULL(test_number,               '${N}', CAST(test_number AS VARCHAR)),               '${D}' ORDER BY test_number)  AS m_test_number,
        LIST(IFNULL(test_type,                 '${N}', CAST(test_type AS VARCHAR)),                 '${D}' ORDER BY test_number)  AS m_test_type,
        LIST(IFNULL(uwi,                       '${N}', CAST(uwi AS VARCHAR)),                       '${D}' ORDER BY test_number)  AS m_uwi
      FROM well_test_mud
      GROUP BY uwi, source, test_type, run_number
    )
    SELECT
      w.*,
      t.*,
      s.*,
      p.*,
      c.*,
      f.*,
      a.*,
      m.*
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
    LEFT OUTER JOIN c
      ON c.id_c_uwi = t.id_t_uwi
      AND c.id_c_source = t.id_t_source
      AND c.id_c_test_type = t.id_t_test_type
      AND c.id_c_run_number = t.id_t_run_number
    LEFT OUTER JOIN f
      ON f.id_f_uwi = t.id_t_uwi
      AND f.id_f_source = t.id_t_source
      AND f.id_f_test_type = t.id_t_test_type
      AND f.id_f_run_number = t.id_t_run_number
    LEFT OUTER JOIN a
      ON a.id_a_uwi = t.id_t_uwi
      AND a.id_a_source = t.id_t_source
      AND a.id_a_test_type = t.id_t_test_type
      AND a.id_a_run_number = t.id_t_run_number
    LEFT OUTER JOIN m
      ON m.id_m_uwi = t.id_t_uwi
      AND m.id_m_source = t.id_t_source
      AND m.id_m_test_type = t.id_t_test_type
      AND m.id_m_run_number = t.id_t_run_number
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
  s_end_press_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  s_gx_duration: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  s_period_type: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  s_quality_code: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  s_remark: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  s_row_changed_date: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_run_number: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  s_source: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  s_start_press: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_start_press_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  s_test_number: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  s_test_type: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  s_uwi: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },

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
  p_period_type: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  p_recovery_amt: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  p_recovery_amt_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
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
  p_run_number: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  p_source: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  p_test_number: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  p_test_type: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  p_uwi: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },

  // WELL_TEST_RECOVERY (CHAMBER)

  id_c_uwi: {
    ts_type: "string",
  },
  id_c_source: {
    ts_type: "string",
  },
  id_c_test_type: {
    ts_type: "string",
  },
  id_c_run_number: {
    ts_type: "string",
  },
  c_period_type: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  c_recovery_amt: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  c_recovery_amt_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  c_recovery_amt_uom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  c_recovery_method: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  c_recovery_obs_no: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  c_recovery_type: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  c_remark: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  c_row_changed_date: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  c_run_number: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  c_source: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  c_test_number: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  c_test_type: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  c_uwi: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },

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
  f_max_fluid_rate_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_max_fluid_rate_uom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_max_surface_press: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_max_surface_press_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_period_obs_no: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_period_type: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_remark: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_row_changed_date: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_run_number: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_source: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_test_number: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_test_type: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_tts_elapsed_time_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_tts_elasped_time: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_uwi: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },

  // WELL_TEST_ANALYSIS

  id_a_uwi: {
    ts_type: "string",
  },
  id_a_source: {
    ts_type: "string",
  },
  id_a_test_type: {
    ts_type: "string",
  },
  id_a_run_number: {
    ts_type: "string",
  },
  a_analysis_obs_no: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  a_bsw: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  a_condensate_gravity: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  a_condensate_gravity_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  a_condensate_ratio: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  a_condensate_ratio_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  a_condensate_temp: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  a_condensate_temp_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  a_fluid_type: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  a_gas_content: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  a_gas_gravity: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  a_gas_gravity_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  a_gor: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  a_gor_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  a_gwr: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  a_gwr_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  a_gx_fluid_percent: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  a_h2s_pct: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  a_lgr: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  a_lgr_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  a_oil_density: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  a_oil_densty_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  a_oil_gravity: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  a_oil_gravity_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  a_oil_temp: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  a_oil_temp_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  a_period_obs_no: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  a_period_type: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  a_row_changed_date: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  a_run_number: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  a_salinity_type: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  a_source: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  a_sulphur_pct: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  a_test_number: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  a_test_type: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  a_uwi: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  a_water_cut: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  a_water_resistivity: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  a_water_resistivity_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  a_water_salinity: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  a_water_salinity_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  a_water_temp: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  a_water_temp_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  a_wor: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },

  // WELL_TEST_MUD

  id_m_uwi: {
    ts_type: "string",
  },
  id_m_source: {
    ts_type: "string",
  },
  id_m_test_type: {
    ts_type: "string",
  },
  id_m_run_number: {
    ts_type: "string",
  },
  m_filtrate_resistivity: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_filtrate_resistivity_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  m_filtrate_temp: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_filtrate_temp_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  m_gx_filter_cake_thickness: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_gx_tracer: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  m_gx_water_loss: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_mud_resistivity: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_mud_resistivity_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  m_mud_temp: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_mud_temp_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  m_mud_type: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  m_mud_viscosity: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_mud_viscosity_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  m_mud_weight: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_mud_weight_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  m_row_changed_date: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  m_run_number: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  m_source: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  m_test_number: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  m_test_type: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  m_uwi: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
};

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

const global_id_keys = [
  "w_uwi",
  "id_t_source",
  "id_t_test_type",
  "id_t_run_number",
];

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
