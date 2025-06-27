tidy_raw_csvs = function(dataList, type){
  
  if(type == "five_hz"){
    data = dataList |> 
      dplyr::bind_rows() |> 
      dplyr::tibble() |> 
      dplyr::select(
        unix_time = TheTime,
        ch1_hz = CH1_Hz,
        ch2_hz = CH2_Hz,
        w_ms = w,
        v_ms = vv,
        u_ms = u,
        tempSonic_ms = temp_sonic
      ) 
  }
  
  if(type == "params"){
    dataList |> 
      dplyr::bind_rows() |> 
      dplyr::tibble() |> 
      dplyr::select(
        unix_time = TheTime, 
        blc_lamp_1 = BLC_Lamp_1, 
        blc_lamp_2 = BLC_Lamp_2, 
        blc_temp = MLC_temp,
        control_temp = Control_Temp,
        hv_1 = HV_1, 
        hv_1_cntl = HV_1_cntl, 
        hv_2 = HV_2,
        hv_2_cntl = HV_2_cntl, 
        inlet_pressure = Inlet_pressure,
        inlet_pressure_set = Inlet_pressure_set,
        nox_cal = NOx_cal, 
        no_cal = NO_cal, 
        no_cal_flow = NO_Cal_flow,
        no_cal_flow_set = NO_Cal_flow_set,
        no_valve = NO_valve,
        oxygen_flow_1 = OxygenFlow_1,
        oxygen_flow_2 = OxygenFlow_2,
        oxygen_valve_1 = oxygen_valve_1,
        oxygen_valve_2 = oxygen_valve_2,
        ozoniser_1 = ozone_1,
        ozoniser_2 = ozone_2,
        pmt_temp = PMT_Temp,
        reaction_cell_temp = Rxn_Cell_Temp,
        reaction_cell_pressure = Rxn_Vessel_Pressure,
        sample_flow_1 = SampleFlow1,
        sample_flow_set_1 = SampleFlow1_set,
        sample_flow_set_2 = SampleFlow2_set,
        vac_valve_1, 
        vac_valve_2,
        zero_valve_1,
        zero_valve_2,
        zero_volume_temperature,
        ozone_reset,
        ozone_trip
      )
  }
  
  data |> 
    dplyr::mutate(unix_time = (unix_time-25569)*86400)
  
}
