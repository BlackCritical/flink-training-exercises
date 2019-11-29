package com.ververica.flinktraining.project.util;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum MagnitudeType {

    /**
     * ALL COMMENTED OUT MAGNITUDE TYPES ARE TYPES WHICH DO NOT OCCUR IN MY DATASET!
     * I REMOVED THEM FOR SIMPLER VISUALIZATION.
     */

    MWW("Mww (Moment W-phase) (generic notation Mw)","~5.0 and larger", new String[]{"MWW", "MW"}),
    MC("1 - 90 degrees","~5.5 and larger", new String[]{"MC"}),
    MWC("1 - 90 degrees","~5.5 and larger", new String[]{"MWC"}),
    MWB("Mwb (body wave)","~5.5 to ~7.0", new String[]{"MWB"}),
    MWR("Mwr (regional)","~4.0 to ~6.5", new String[]{"MWR"}),
    MS("Ms20 or Ms (20sec surface wave)","~5.0 to ~8.5", new String[]{"MS","MS_20"}),
    MB("mb (shortperiod body wave)","~4.0 to ~6.5", new String[]{"MB"}),
//    MFA("Mfa (feltarea magnitude)","any", new String[]{"MFA"}),
    ML("ML Ml, or ml (local)","~2.0 to ~6.5", new String[]{"ML", "MLR"}),
    MB_LG("mb_Lg, mb_lg, or MLg (shortperiod surface wave)","~3.5 to ~7.0", new String[]{"MB_LG", "MLG"}),
    MD("Md or md (duration)","~4 or smaller", new String[]{"MD"}),
//    MWP("Mi or Mwp (integrated p-wave)","~5.0 to ~8.0", new String[]{"MWP"}),
//    ME("Me (energy)","~3.5 and larger", new String[]{"ME"}),
    MH("Mh","any", new String[]{"MH", "H"});
//    FINITE_FAULT("Finite Fault Modeling","~7.0 and larger", new String[]{"FINITE_FAULT"}),
//    MINT("Mint (intensity magnitude)","any", new String[]{"MINT"});

    private String description;
    private String range;
    private String[] shortForms;
}
