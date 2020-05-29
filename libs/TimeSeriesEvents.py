import pandas as pd
import numpy as np
def FlagDeviceConsumableEvents(df_in, device_col, cons_perc_col, cons_type_col, time_col, lst_output_cols, units_duration):

    """
    Flags status and change events for a consumable component (e.g. of an internet-connected device) based on
    values in a percent-consumed indicator column, cons_perc_col.  cons_perc_col=0 signifies an empty consumable.
    Code assumes data are in blocks differentiated by a Device ID, device_col and [optionally] by a
    consumable "Type", type_col, for which changes within a device's data represent replacement of the
    consumable.  Such replacements can occur at arbitrary cons_perc_col values.

    Function Arguments:
    df_in: DataFrame containing the event data; sorted by device_col and time_col
    device_col: column name that differentiates different devices
    cons_perc_col: Column name containing consumable's percentage remaining
    cons_type_col: Type of consumable; changes in type represent a consumable changeout
    time_col: datetime format timestamp
    lst_output_cols:  list containing names to use for created columns
    units_duration:  specifies units for calculated duration:  'seconds', 'minutes', 'hours', 'days'

    JDL 5/29/20
    """
    df = df_in.copy()
    df.sort_values([device_col,time_col],inplace=True)

    cons_out_col = lst_output_cols[0] #Flag consumable-removed events
    cons_in_col = lst_output_cols[1] #Flag consumable-replaced events
    cons_dur_in_col = lst_output_cols[2] #Consumable duration in (logged at removal)
    cons_dur_out_col = lst_output_cols[3] #Consumable duration out (logged when consumable is replaced after removal)
    cons_prev_perc_col = lst_output_cols[4] #Consumable previous (starting) percentage logged for removal events
    durtemp_col = 'duration' #Temporary duration column

    #Filter to rows with consumable percent-remaining events only; Mask has same index as df
    mask_cons_perc_events = ~df[cons_perc_col].isnull()

    #Create previous and next, shift DataFrames with columns of interest
    df_shift_prev = df.loc[mask_cons_perc_events, [device_col, cons_perc_col, cons_type_col]].shift(periods=1)
    df_shift_next = df.loc[mask_cons_perc_events, [device_col, cons_perc_col, cons_type_col]].shift(periods=-1)

    #Filtered on/off signals - blank out shifted rows where device_col differs from previous device_col value
    mask_devcol_changed = df.loc[mask_cons_perc_events, device_col] != df_shift_prev[device_col]
    mask_devcol_changed2 = df.loc[mask_cons_perc_events, device_col] != df_shift_next[device_col]

    mask_type_chng_prev = df.loc[mask_cons_perc_events, cons_type_col] != df_shift_prev[cons_type_col]
    mask_type_chng_next = df.loc[mask_cons_perc_events, cons_type_col] != df_shift_next[cons_type_col]

    #Type 1 combined OnOff Events mask - Off: goes from not 0 to 0; On: goes from 0 to not 0 (ignores perfume type)
    maskOnEvents1 = mask_cons_perc_events & (df[cons_perc_col]!=0) & (df_shift_prev[cons_perc_col]==0)
    maskOffEvents1 = mask_cons_perc_events & (df[cons_perc_col]==0) & (df_shift_prev[cons_perc_col]!=0)
    maskOnOff1 = (maskOnEvents1 | maskOffEvents1)

    #Type 2 - On events can be marked even in first row of a new device so doesn't include "& (~mask_devcol_changed)" for On
    maskOnEvents2 = (mask_cons_perc_events) & ((mask_type_chng_prev) | (mask_devcol_changed)) & (df[cons_perc_col] > 0.0)
    maskOffEvents2 = (mask_cons_perc_events) & ((mask_type_chng_next) | (mask_devcol_changed2))
    maskOnOff2 = (maskOnEvents2 | maskOffEvents2)

    maskOnEvents = maskOnEvents1 | maskOnEvents2
    maskOffEvents = maskOffEvents1 | maskOffEvents2
    maskOnOff = maskOnOff1 | maskOnOff2

    #Calculate duration in minutes (temporary column mixing in and out-durations)
    df.loc[maskOnOff, durtemp_col] = df.loc[maskOnOff, time_col] - df.loc[maskOnOff, time_col].shift(periods=1)

    f_tunits = 1.0 #seconds
    if units_duration == 'minutes': f_tunits = 1.0/60.0
    if units_duration == 'hours': f_tunits = 1.0/3600.0
    if units_duration == 'days': f_tunits = 1.0/86400.0
    df[durtemp_col] = df[durtemp_col].astype('timedelta64[s]') * f_tunits

    #Populate new columns
    df.loc[maskOffEvents, cons_out_col] = 1.0
    df.loc[maskOnEvents, cons_in_col] = 1.0

    #Delete edge effect durations and event flags for rows where device column changed
    mask_edge_effects = (mask_cons_perc_events & mask_devcol_changed)
    mask_edge_effects_out = mask_edge_effects & (df[cons_out_col]==1.0)

    df.loc[mask_edge_effects, durtemp_col] = np.nan
    df.loc[mask_edge_effects_out, cons_out_col] = np.nan

    #Populate previous consumable percent-remaining column to identify starting percent for removal events
    maskOnOff = (~df[cons_in_col].isnull()) | (~df[cons_out_col].isnull())
    maskOnEvents = ~df[cons_in_col].isnull()

    df.loc[maskOnOff, cons_prev_perc_col] = df.loc[maskOnOff, cons_perc_col].shift(periods=1)
    df.loc[maskOnEvents, cons_prev_perc_col] = np.nan

    #Move duration to separate duration on/off columns and delete temp column
    df.loc[(mask_cons_perc_events & mask_devcol_changed), durtemp_col] = np.nan
    df.loc[maskOffEvents, cons_dur_in_col] = df.loc[maskOffEvents, durtemp_col]
    df.loc[maskOnEvents, cons_dur_out_col] = df.loc[maskOnEvents, durtemp_col]
    df.drop(durtemp_col, axis=1, inplace=True)
    return df
