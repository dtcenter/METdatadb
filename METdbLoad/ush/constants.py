#!/usr/bin/env python3
"""Define constants."""

# pylint:disable=no-member
# constants exist in constants.py

# name to use for a group when no group tag is included in load_spec
DEFAULT_DATABASE_GROUP = "NO GROUP"

# Maximum number of columns
MAX_COL = 120

# Maximum number of files to load at a time
# Goal is to not max out memory
MAX_FILES = 1000

COL_NUMS = [str(x) for x in range(MAX_COL - 24)]

MYSQL = "mysql"
MARIADB = "mariadb"
AURORA = "aurora"
RELATIONAL = [MYSQL, MARIADB, AURORA]

CB = "cb"

# default port for MySQL
SQL_PORT = 3306

# Lower Case true and false
LC_TRUE = "true"
LC_FALSE = "false"

# Not Available - NA
NOTAV = 'NA'

# METviewer value for Not Available
MV_NOTAV = '-9999'

# Used for null fields in mode_header - needed by LOAD DATA INFILE
MV_NULL = '\\N'

# separator for csv files
SEP = '$'

# Equal Sign
EQS = '='

# Forward Slash
FWD_SLASH = '/'

# Left paren for searching
L_PAREN = '('

# Right paren for searching
R_PAREN = ')'

# Generic count of variable fields
N_VAR = 'n_var'

# No key is a number that would not be a valid key that is put in as a placeholder
NO_KEY = -1

# names for columns that contain not available and zero values
COL_NA = 'colna'
COL_ZERO = 'zeroes'

# repeat COL_NA for building VSDB dataframe
COL_NAS = [COL_NA] * 95

# STAT line types - comments from the v8.1.1 MET user's guide
FHO = "FHO"        # Forecast, Hit, Observation Rates
CTC = "CTC"        # Contingency Table Counts
CTS = "CTS"        # Contingency Table Statistics
MCTC = "MCTC"      # Multi-category Contingency Table Counts
MCTS = "MCTS"      # Multi-category Contingency Table Statistics
CNT = "CNT"        # Continuous Statistics
SL1L2 = "SL1L2"    # Scalar L1L2 Partial Sums
SAL1L2 = "SAL1L2"  # Scalar Anomaly L1L2 Partial Sums when climatological data is supplied
VL1L2 = "VL1L2"    # Vector L1L2 Partial Sums
VAL1L2 = "VAL1L2"  # Vector Anomaly L1L2 Partial Sums when climatological data is supplied
PCT = "PCT"        # Contingency Table Counts for Probabilistic Forecasts
PSTD = "PSTD"      # Contingency Table Stats for Probabilistic Forecasts with Dichotomous outcomes
PJC = "PJC"        # Joint and Conditional Factorization for Probabilistic Forecasts
PRC = "PRC"        # Receiver Operating Characteristic for Probabilistic Forecasts
ECLV = "ECLV"      # Economic Cost/Loss Value derived from CTC and PCT lines
MPR = "MPR"        # Matched Pair Data
NBRCTC = "NBRCTC"  # Neighborhood Contingency Table Counts
NBRCTS = "NBRCTS"  # Neighborhood Contingency Table Statistics
NBRCNT = "NBRCNT"  # Neighborhood Continuous Statistics
ISC = "ISC"        # Intensity-Scale
RHIST = "RHIST"    # Ranked Histogram
PHIST = "PHIST"    # Probability Integral Transform Histogram
ORANK = "ORANK"    # Observation Rank
SSVAR = "SSVAR"    # Spread Skill Variance
GRAD = "GRAD"      # Gradient statistics (S1 score)
VCNT = "VCNT"      # Vector Continuous Statistics
RELP = "RELP"      # Relative Position
ECNT = "ECNT"      # Ensemble Continuous Statistics - only for HiRA
ENSCNT = "ENSCNT"  #
PERC = "PERC"      #

# VSDB line types
BSS = "BSS"        # same as PSTD
RELI = "RELI"      # same as PCT
HIST = "HIST"      # same as RHIST
ECON = "ECON"      # same as ECLV
RMSE = "RMSE"      # same as CNT
RPS = "RPS"        # same as ENSCNT
FSS = "FSS"        # same as NBRCNT
# VSDB version of FHO goes to CTC
# VSDB versions of RELP, SL1L2, SAL1L2, VL1L2, VAL1L2, and GRAD do not change

UC_LINE_TYPES = [FHO, CTC, CTS, MCTC, MCTS, CNT, SL1L2, SAL1L2, VL1L2, VAL1L2,
                 PCT, PSTD, PJC, PRC, ECLV, MPR, NBRCTC, NBRCTS, NBRCNT, ISC,
                 RHIST, PHIST, ORANK, SSVAR, GRAD, VCNT, RELP, ECNT, ENSCNT, PERC]

LC_LINE_TYPES = [ltype.lower() for ltype in UC_LINE_TYPES]

LINE_TABLES = ['line_data_' + hname for hname in LC_LINE_TYPES]

ALPHA_LINE_TYPES = [CTS, CNT, PSTD, NBRCTS, NBRCNT, MCTS, SSVAR, VCNT]

COV_THRESH_LINE_TYPES = [NBRCTC, NBRCTS, PCT, PSTD, PJC, PRC]

VAR_LINE_TYPES = [PCT, PSTD, PJC, PRC, MCTC, RHIST, PHIST, RELP, ORANK, ECLV]

OLD_VSDB_LINE_TYPES = [BSS, ECON, HIST, RELI, RMSE, RPS, FHO, FSS]

VSDB_TO_STAT_TYPES = [PSTD, ECLV, RHIST, PCT, CNT, ENSCNT, CTC, NBRCNT]

ENS_VSDB_LINE_TYPES = [BSS, ECON, HIST, RELI, RELP, RMSE, RPS]

ALL_VSDB_LINE_TYPES = OLD_VSDB_LINE_TYPES + [RELP, SL1L2, SAL1L2, VL1L2, VAL1L2, GRAD]

# column names
# MET column names are UC, SQL are LC
UC_DESC = "DESC"
UC_FCST_UNITS = "FCST_UNITS"

VERSION = "version"
MODEL = "model"
# MET file contains DESC. SQL field name is descr
DESCR = "descr"
FCST_LEAD = "fcst_lead"
FCST_VALID_BEG = "fcst_valid_beg"
FCST_VALID_END = "fcst_valid_end"
OBS_LEAD = "obs_lead"
OBS_VALID_BEG = "obs_valid_beg"
OBS_VALID_END = "obs_valid_end"
FCST_VAR = "fcst_var"
FCST_UNITS = "fcst_units"
FCST_LEV = "fcst_lev"
OBS_VAR = "obs_var"
OBS_UNITS = "obs_units"
OBS_LEV = "obs_lev"
OBTYPE = "obtype"
VX_MASK = "vx_mask"
INTERP_MTHD = "interp_mthd"
INTERP_PNTS = "interp_pnts"
FCST_THRESH = "fcst_thresh"
OBS_THRESH = "obs_thresh"
COV_THRESH = "cov_thresh"
ALPHA = "alpha"
LINE_TYPE = "line_type"
FCST_INIT_BEG = "fcst_init_beg"

FCST_LEAD_HR = "fcst_lead_hr"

# After units added in MET 8.1
LONG_HEADER = [VERSION, MODEL, DESCR, FCST_LEAD, FCST_VALID_BEG, FCST_VALID_END,
               OBS_LEAD, OBS_VALID_BEG, OBS_VALID_END, FCST_VAR, FCST_UNITS, FCST_LEV,
               OBS_VAR, OBS_UNITS, OBS_LEV, OBTYPE, VX_MASK, INTERP_MTHD, INTERP_PNTS,
               FCST_THRESH, OBS_THRESH, COV_THRESH, ALPHA, LINE_TYPE]

# Contains DESC but not UNITS
MID_HEADER = LONG_HEADER[0:10] + LONG_HEADER[11:13] + LONG_HEADER[14:]

# No DESC and no UNITS
SHORT_HEADER = MID_HEADER[0:2] + MID_HEADER[3:]

STAT_HEADER_KEYS = [VERSION, MODEL, DESCR, FCST_VAR, FCST_UNITS, FCST_LEV,
                    OBS_VAR, OBS_UNITS, OBS_LEV, OBTYPE, VX_MASK,
                    INTERP_MTHD, INTERP_PNTS, FCST_THRESH, OBS_THRESH]

VSDB_HEADER = [VERSION, MODEL, FCST_LEAD, FCST_VALID_BEG, OBTYPE,
               VX_MASK, LINE_TYPE, FCST_VAR, FCST_LEV]

Q_FILE = "SELECT data_file_id FROM data_file WHERE " + \
         "path=%s AND filename=%s"

Q_HEADER = "SELECT stat_header_id FROM stat_header WHERE " + \
           "=%s AND ".join(STAT_HEADER_KEYS) + "=%s"

Q_METADATA = "SELECT category, description FROM metadata"

STAT_HEADER = 'stat_header'
STAT_HEADER_ID = 'stat_header_id'
LINE_DATA_ID = 'line_data_id'
LINE_NUM = 'line_num'
TOTAL_LC = 'total'
FCST_PERC = 'fcst_perc'
OBS_PERC = 'obs_perc'

DATA_FILE = 'data_file'
FULL_FILE = 'full_file'
DATA_FILE_ID = 'data_file_id'
DATA_FILE_LU_ID = 'data_file_lu_id'
FILE_ROW = 'file_row'
FILENAME = 'filename'
FILEPATH = 'path'
LOAD_DATE = 'load_date'
MOD_DATE = 'mod_date'
FY_OY = 'fy_oy'
FY_ON = 'fy_on'
FN_OY = 'fn_oy'
FN_ON = 'fn_on'

INSTANCE_INFO = 'instance_info'
INSTANCE_INFO_ID = 'instance_info_id'

DATA_FILE_FIELDS = [DATA_FILE_ID, DATA_FILE_LU_ID, FILENAME, FILEPATH,
                    LOAD_DATE, MOD_DATE]

STAT_HEADER_FIELDS = [STAT_HEADER_ID, VERSION, MODEL, DESCR,
                      FCST_VAR, FCST_UNITS, FCST_LEV,
                      OBS_VAR, OBS_UNITS, OBS_LEV,
                      OBTYPE, VX_MASK, INTERP_MTHD, INTERP_PNTS,
                      FCST_THRESH, OBS_THRESH]

VALUE_SLOTS = '%s, ' * len(STAT_HEADER_FIELDS)
VALUE_SLOTS = VALUE_SLOTS[:-2]

INS_HEADER = "INSERT INTO stat_header (" + ",".join(STAT_HEADER_FIELDS) + \
             ") VALUES (" + VALUE_SLOTS + ")"

INS_DATA_FILES = "INSERT INTO data_file (" + ",".join(DATA_FILE_FIELDS) + \
                 ") VALUES (%s, %s, %s, %s, %s, %s)"

INS_METADATA = "INSERT INTO metadata (category, description) VALUES (%s, %s)"

INS_INSTANCE = "INSERT INTO instance_info (instance_info_id, updater, update_date, " + \
               "update_detail, load_xml) VALUES (%s, %s, %s, %s, %s)"

UPD_METADATA = "UPDATE metadata SET category=%s, description=%s"

LD_TABLE = "LOAD DATA LOCAL INFILE '{}' INTO TABLE {} FIELDS TERMINATED BY '{}';"

ALL_LINE_DATA_FIELDS = [STAT_HEADER_ID, DATA_FILE_ID, LINE_NUM,
                        FCST_LEAD, FCST_VALID_BEG, FCST_VALID_END, FCST_INIT_BEG,
                        OBS_LEAD, OBS_VALID_BEG, OBS_VALID_END]

TOT_LINE_DATA_FIELDS = ALL_LINE_DATA_FIELDS + [TOTAL_LC]

ALPH_LINE_DATA_FIELDS = ALL_LINE_DATA_FIELDS + [ALPHA, TOTAL_LC]

COV_LINE_DATA_FIELDS = ALL_LINE_DATA_FIELDS + [COV_THRESH, TOTAL_LC]

COVA_LINE_DATA_FIELDS = ALL_LINE_DATA_FIELDS + [COV_THRESH, ALPHA, TOTAL_LC]

ALL_COUNT = len(ALL_LINE_DATA_FIELDS)

LINE_DATA_FIELDS = dict()
LINE_DATA_VAR_FIELDS = dict()
LINE_DATA_COLS = dict()
LINE_DATA_Q = dict()
LINE_DATA_VAR_Q = dict()
LINE_VAR_COUNTER = dict()
LINE_VAR_REPEATS = dict()
LINE_DATA_VAR_TABLES = dict()

LINE_DATA_VAR_TABLES[PCT] = 'line_data_pct_thresh'
LINE_DATA_VAR_TABLES[PSTD] = 'line_data_pstd_thresh'
LINE_DATA_VAR_TABLES[PJC] = 'line_data_pjc_thresh'
LINE_DATA_VAR_TABLES[PRC] = 'line_data_prc_thresh'
LINE_DATA_VAR_TABLES[MCTC] = 'line_data_mctc_cnt'
LINE_DATA_VAR_TABLES[RHIST] = 'line_data_rhist_rank'
LINE_DATA_VAR_TABLES[RELP] = 'line_data_relp_ens'
LINE_DATA_VAR_TABLES[PHIST] = 'line_data_phist_bin'
LINE_DATA_VAR_TABLES[ORANK] = 'line_data_orank_ens'
LINE_DATA_VAR_TABLES[ECLV] = 'line_data_eclv_pnt'

LINE_DATA_FIELDS[CNT] = ALPH_LINE_DATA_FIELDS + \
                        ['fbar', 'fbar_ncl', 'fbar_ncu', 'fbar_bcl', 'fbar_bcu',
                         'fstdev', 'fstdev_ncl', 'fstdev_ncu', 'fstdev_bcl', 'fstdev_bcu',
                         'obar', 'obar_ncl', 'obar_ncu', 'obar_bcl', 'obar_bcu',
                         'ostdev', 'ostdev_ncl', 'ostdev_ncu', 'ostdev_bcl', 'ostdev_bcu',
                         'pr_corr', 'pr_corr_ncl', 'pr_corr_ncu', 'pr_corr_bcl', 'pr_corr_bcu',
                         'sp_corr', 'dt_corr', 'ranks', 'frank_ties', 'orank_ties',
                         'me', 'me_ncl', 'me_ncu', 'me_bcl', 'me_bcu',
                         'estdev', 'estdev_ncl', 'estdev_ncu', 'estdev_bcl', 'estdev_bcu',
                         'mbias', 'mbias_bcl', 'mbias_bcu', 'mae', 'mae_bcl', 'mae_bcu',
                         'mse', 'mse_bcl', 'mse_bcu', 'bcmse', 'bcmse_bcl', 'bcmse_bcu',
                         'rmse', 'rmse_bcl', 'rmse_bcu', 'e10', 'e10_bcl', 'e10_bcu',
                         'e25', 'e25_bcl', 'e25_bcu', 'e50', 'e50_bcl', 'e50_bcu',
                         'e75', 'e75_bcl', 'e75_bcu', 'e90', 'e90_bcl', 'e90_bcu',
                         'iqr', 'iqr_bcl', 'iqr_bcu', 'mad', 'mad_bcl', 'mad_bcu',
                         'anom_corr', 'anom_corr_ncl', 'anom_corr_ncu',
                         'anom_corr_bcl', 'anom_corr_bcu',
                         'me2', 'me2_bcl', 'me2_bcu', 'msess', 'msess_bcl', 'msess_bcu',
                         'rmsfa', 'rmsfa_bcl', 'rmsfa_bcu', 'rmsoa', 'rmsoa_bcl', 'rmsoa_bcu']

LINE_DATA_FIELDS[CTC] = TOT_LINE_DATA_FIELDS + \
                        [FY_OY, FY_ON, FN_OY, FN_ON]

LINE_DATA_FIELDS[CTS] = ALPH_LINE_DATA_FIELDS + \
                        ['baser', 'baser_ncl', 'baser_ncu', 'baser_bcl', 'baser_bcu',
                         'fmean', 'fmean_ncl', 'fmean_ncu', 'fmean_bcl', 'fmean_bcu',
                         'acc', 'acc_ncl', 'acc_ncu', 'acc_bcl', 'acc_bcu',
                         'fbias', 'fbias_bcl', 'fbias_bcu',
                         'pody', 'pody_ncl', 'pody_ncu', 'pody_bcl', 'pody_bcu',
                         'podn', 'podn_ncl', 'podn_ncu', 'podn_bcl', 'podn_bcu',
                         'pofd', 'pofd_ncl', 'pofd_ncu', 'pofd_bcl', 'pofd_bcu',
                         'far', 'far_ncl', 'far_ncu', 'far_bcl', 'far_bcu',
                         'csi', 'csi_ncl', 'csi_ncu', 'csi_bcl', 'csi_bcu',
                         'gss', 'gss_bcl', 'gss_bcu',
                         'hk', 'hk_ncl', 'hk_ncu', 'hk_bcl', 'hk_bcu',
                         'hss', 'hss_bcl', 'hss_bcu',
                         'odds', 'odds_ncl', 'odds_ncu', 'odds_bcl', 'odds_bcu',
                         'lodds', 'lodds_ncl', 'lodds_ncu', 'lodds_bcl', 'lodds_bcu',
                         'orss', 'orss_ncl', 'orss_ncu', 'orss_bcl', 'orss_bcu',
                         'eds', 'eds_ncl', 'eds_ncu', 'eds_bcl', 'eds_bcu',
                         'seds', 'seds_ncl', 'seds_ncu', 'seds_bcl', 'seds_bcu',
                         'edi', 'edi_ncl', 'edi_ncu', 'edi_bcl', 'edi_bcu',
                         'sedi', 'sedi_ncl', 'sedi_ncu', 'sedi_bcl', 'sedi_bcu',
                         'bagss', 'bagss_bcl', 'bagss_bcu']

LINE_DATA_FIELDS[ECLV] = TOT_LINE_DATA_FIELDS + \
                         ['baser', 'value_baser', 'n_pnt']

LINE_DATA_FIELDS[ECNT] = TOT_LINE_DATA_FIELDS + \
                         ['n_ens', 'crps', 'crpss', 'ign', 'me', 'rmse', 'spread',
                          'me_oerr', 'rmse_oerr', 'spread_oerr', 'spread_plus_oerr']

LINE_DATA_FIELDS[ENSCNT] = ALL_LINE_DATA_FIELDS + \
                           ['rpsf', 'rpsf_ncl', 'rpsf_ncu', 'rpsf_bcl', 'rpsf_bcu',
                            'rpscl', 'rpscl_ncl', 'rpscl_ncu', 'rpscl_bcl', 'rpscl_bcu',
                            'rpss', 'rpss_ncl', 'rpss_ncu', 'rpss_bcl', 'rpss_bcu',
                            'crpsf', 'crpsf_ncl', 'crpsf_ncu', 'crpsf_bcl', 'crpsf_bcu',
                            'crpscl', 'crpscl_ncl', 'crpscl_ncu', 'crpscl_bcl', 'crpscl_bcu',
                            'crpss', 'crpss_ncl', 'crpss_ncu', 'crpss_bcl', 'crpss_bcu']

LINE_DATA_FIELDS[FHO] = TOT_LINE_DATA_FIELDS + \
                        ['f_rate', 'h_rate', 'o_rate']

LINE_DATA_FIELDS[GRAD] = TOT_LINE_DATA_FIELDS + \
                         ['fgbar', 'ogbar', 'mgbar', 'egbar', 's1', 's1_og', 'fgog_ratio',
                          'dx', 'dy']

LINE_DATA_FIELDS[ISC] = TOT_LINE_DATA_FIELDS + \
                        ['tile_dim', 'time_xll', 'tile_yll', 'nscale', 'iscale', 'mse,isc']

LINE_DATA_FIELDS[MCTC] = TOT_LINE_DATA_FIELDS + \
                         ['n_cat']

LINE_DATA_FIELDS[MCTS] = ALPH_LINE_DATA_FIELDS + \
                         ['n_cat', 'acc', 'acc_ncl', 'acc_ncu', 'acc_bcl', 'acc_bcu',
                          'hk', 'hk_bcl', 'hk_bcu', 'hss', 'hss_bcl', 'hss_bcu',
                          'ger', 'ger_bcl', 'ger_bcu']

LINE_DATA_FIELDS[MPR] = TOT_LINE_DATA_FIELDS + \
                         ['mp_index', 'obs_sid', 'obs_lat', 'obs_lon', 'obs_lvl', 'obs_elv',
                          'mpr_fcst', 'mpr_obs', 'mpr_climo', 'obs_qc',
                          'climo_mean', 'climo_stdev', 'climo_cdf']

LINE_DATA_FIELDS[NBRCNT] = ALPH_LINE_DATA_FIELDS + \
                           ['fbs', 'fbs_bcl', 'fbs_bcu', 'fss', 'fss_bcl', 'fss_bcu',
                            'afss', 'afss_bcl', 'afss_bcu', 'ufss', 'ufss_bcl', 'ufss_bcu',
                            'f_rate', 'f_rate_bcl', 'f_rate_bcu',
                            'o_rate', 'o_rate_bcl', 'o_rate_bcu']

LINE_DATA_FIELDS[NBRCTC] = COV_LINE_DATA_FIELDS + \
                           [FY_OY, FY_ON, FN_OY, FN_ON]

LINE_DATA_FIELDS[NBRCTS] = COVA_LINE_DATA_FIELDS + \
                           ['baser', 'baser_ncl', 'baser_ncu', 'baser_bcl', 'baser_bcu',
                            'fmean', 'fmean_ncl', 'fmean_ncu', 'fmean_bcl', 'fmean_bcu',
                            'acc', 'acc_ncl', 'acc_ncu', 'acc_bcl', 'acc_bcu',
                            'fbias', 'fbias_bcl', 'fbias_bcu',
                            'pody', 'pody_ncl', 'pody_ncu', 'pody_bcl', 'pody_bcu',
                            'podn', 'podn_ncl', 'podn_ncu', 'podn_bcl', 'podn_bcu',
                            'pofd', 'pofd_ncl', 'pofd_ncu', 'pofd_bcl', 'pofd_bcu',
                            'far', 'far_ncl', 'far_ncu', 'far_bcl', 'far_bcu',
                            'csi', 'csi_ncl', 'csi_ncu', 'csi_bcl', 'csi_bcu',
                            'gss', 'gss_bcl', 'gss_bcu',
                            'hk', 'hk_ncl', 'hk_ncu', 'hk_bcl', 'hk_bcu',
                            'hss', 'hss_bcl', 'hss_bcu',
                            'odds', 'odds_ncl', 'odds_ncu', 'odds_bcl', 'odds_bcu',
                            'lodds', 'lodds_ncl', 'lodds_ncu', 'lodds_bcl', 'lodds_bcu',
                            'orss', 'orss_ncl', 'orss_ncu', 'orss_bcl', 'orss_bcu',
                            'eds', 'eds_ncl', 'eds_ncu', 'eds_bcl', 'eds_bcu',
                            'seds', 'seds_ncl', 'seds_ncu', 'seds_bcl', 'seds_bcu',
                            'edi', 'edi_ncl', 'edi_ncu', 'edi_bcl', 'edi_bcu',
                            'sedi', 'sedi_ncl', 'sedi_ncu', 'sedi_bcl', 'sedi_bcu',
                            'bagss', 'bagss_bcl', 'bagss_bcu']

LINE_DATA_FIELDS[ORANK] = TOT_LINE_DATA_FIELDS + \
                          ['orank_index', 'obs_sid', 'obs_lat', 'obs_lon', 'obs_lvl', 'obs_elv',
                           'obs', 'pit', 'rank', 'n_ens_vld', 'n_ens', 'obs_qc', 'ens_mean',
                           'climo', 'spread', 'ens_mean_oerr', 'spread_oerr', 'spread_plus_oerr']

LINE_DATA_FIELDS[PCT] = COV_LINE_DATA_FIELDS + \
                        ['n_thresh']

LINE_DATA_FIELDS[PERC] = ALL_LINE_DATA_FIELDS + \
                         ['fcst_perc', 'obs_perc']

LINE_DATA_FIELDS[PHIST] = TOT_LINE_DATA_FIELDS + \
                          ['bin_size', 'n_bin']

LINE_DATA_FIELDS[PJC] = COV_LINE_DATA_FIELDS + \
                        ['n_thresh']

LINE_DATA_FIELDS[PRC] = COV_LINE_DATA_FIELDS + \
                        ['n_thresh']

# the last 5 fields are currently (August 2019) blank in stat files, filled in in write_stat_sql
LINE_DATA_FIELDS[PSTD] = COVA_LINE_DATA_FIELDS + \
                         ['n_thresh', 'baser', 'baser_ncl', 'baser_ncu',
                          'reliability', 'resolution', 'uncertainty', 'roc_auc',
                          'brier', 'brier_ncl', 'brier_ncu', 'briercl', 'briercl_ncl',
                          'briercl_ncu', 'bss', 'bss_smpl']

LINE_DATA_FIELDS[RELP] = TOT_LINE_DATA_FIELDS + \
                         ['n_ens']

LINE_DATA_FIELDS[RHIST] = TOT_LINE_DATA_FIELDS + \
                          ['n_rank']

LINE_DATA_FIELDS[SL1L2] = TOT_LINE_DATA_FIELDS + \
                          ['fbar', 'obar', 'fobar', 'ffbar', 'oobar', 'mae']

LINE_DATA_FIELDS[SAL1L2] = TOT_LINE_DATA_FIELDS + \
                           ['fabar', 'oabar', 'foabar', 'ffabar', 'ooabar', 'mae']

LINE_DATA_FIELDS[SSVAR] = ALPH_LINE_DATA_FIELDS + \
                          ['n_bin', 'bin_i', 'bin_n', 'var_min', 'var_max', 'var_mean',
                           'fbar', 'obar', 'fobar', 'ffbar', 'oobar',
                           'fbar_ncl', 'fbar_ncu', 'fstdev', 'fstdev_ncl', 'fstdev_ncu',
                           'obar_ncl', 'obar_ncu', 'ostdev', 'ostdev_ncl', 'ostdev_ncu',
                           'pr_corr', 'pr_corr_ncl', 'pr_corr_ncu', 'me', 'me_ncl', 'me_ncu',
                           'estdev', 'estdev_ncl', 'estdev_ncu', 'mbias', 'mse', 'bcmse', 'rmse']

LINE_DATA_FIELDS[VL1L2] = TOT_LINE_DATA_FIELDS + \
                          ['ufbar', 'vfbar', 'uobar', 'vobar', 'uvfobar', 'uvffbar',
                           'uvoobar', 'f_speed_bar', 'o_speed_bar']

LINE_DATA_FIELDS[VAL1L2] = TOT_LINE_DATA_FIELDS + \
                           ['ufabar', 'vfabar', 'uoabar', 'voabar', 'uvfoabar', 'uvffabar',
                            'uvooabar']

LINE_DATA_FIELDS[VCNT] = ALPH_LINE_DATA_FIELDS + \
                         ['fbar', 'fbar_bcl', 'fbar_bcu', 'obar', 'obar_bcl', 'obar_bcu',
                          'fs_rms', 'fs_rms_bcl', 'fs_rms_bcu',
                          'os_rms', 'os_rms_bcl', 'os_rms_bcu',
                          'msve', 'msve_bcl', 'msve_bcu', 'rmsve', 'rmsve_bcl', 'rmsve_bcu',
                          'fstdev', 'fstdev_bcl', 'fstdev_bcu',
                          'ostdev', 'ostdev_bcl', 'ostdev_bcu',
                          'fdir', 'fdir_bcl', 'fdir_bcu', 'odir', 'odir_bcl', 'odir_bcu',
                          'fbar_speed', 'fbar_speed_bcl', 'fbar_speed_bcu',
                          'obar_speed', 'obar_speed_bcl', 'obar_speed_bcu',
                          'vdiff_speed', 'vdiff_speed_bcl', 'vdiff_speed_bcu',
                          'vdiff_dir', 'vdiff_dir_bcl', 'vdiff_dir_bcu',
                          'speed_err', 'speed_err_bcl', 'speed_err_bcu',
                          'speed_abserr', 'speed_abserr_bcl', 'speed_abserr_bcu',
                          'dir_err', 'dir_err_bcl', 'dir_err_bcu',
                          'dir_abserr', 'dir_abserr_bcl', 'dir_abserr_bcu']

VAR_DATA_FIELDS = [LINE_DATA_ID, 'i_value']

LINE_DATA_VAR_FIELDS[PCT] = VAR_DATA_FIELDS + ['thresh_i', 'oy_i', 'on_i']

LINE_DATA_VAR_FIELDS[PSTD] = VAR_DATA_FIELDS + ['thresh_i']

LINE_DATA_VAR_FIELDS[PJC] = VAR_DATA_FIELDS + \
                               ['thresh_i', 'oy_tp_i', 'on_tp_i', 'calibration_i',
                                'refinement_i', 'likelihood_i', 'baser_i']

LINE_DATA_VAR_FIELDS[PRC] = VAR_DATA_FIELDS + ['thresh_i', 'pody_i', 'pofd_i']

LINE_DATA_VAR_FIELDS[MCTC] = VAR_DATA_FIELDS + ['j_value', 'fi_oj']

LINE_DATA_VAR_FIELDS[RHIST] = VAR_DATA_FIELDS + ['rank_i']

LINE_DATA_VAR_FIELDS[RELP] = VAR_DATA_FIELDS + ['ens_i']

LINE_DATA_VAR_FIELDS[PHIST] = VAR_DATA_FIELDS + ['bin_i']

LINE_DATA_VAR_FIELDS[ORANK] = VAR_DATA_FIELDS + ['ens_i']

LINE_DATA_VAR_FIELDS[ECLV] = VAR_DATA_FIELDS + ['x_pnt_i', 'y_pnt_i']

for line_type in UC_LINE_TYPES:
    # for each line type, create a list of the columns in the dataframe
    if line_type in COV_THRESH_LINE_TYPES:
        # both alpha and cov_thresh
        if line_type in ALPHA_LINE_TYPES:
            LINE_DATA_COLS[line_type] = \
                COVA_LINE_DATA_FIELDS[0:-1] + \
                COL_NUMS[0:(len(LINE_DATA_FIELDS[line_type]) - (ALL_COUNT + 2))]
        else:
            LINE_DATA_COLS[line_type] = \
                COV_LINE_DATA_FIELDS[0:-1] + \
                COL_NUMS[0:(len(LINE_DATA_FIELDS[line_type]) - (ALL_COUNT + 1))]
    elif line_type in ALPHA_LINE_TYPES:
        LINE_DATA_COLS[line_type] = \
            ALPH_LINE_DATA_FIELDS[0:-1] + \
            COL_NUMS[0:(len(LINE_DATA_FIELDS[line_type]) - (ALL_COUNT + 1))]
    else:
        LINE_DATA_COLS[line_type] = \
            ALL_LINE_DATA_FIELDS + \
             COL_NUMS[0:(len(LINE_DATA_FIELDS[line_type]) - ALL_COUNT)]

    if line_type in VAR_LINE_TYPES:
        LINE_DATA_COLS[line_type] = [LINE_DATA_ID] + LINE_DATA_COLS[line_type]

    # For each line type, create insert queries
    VALUE_SLOTS = '%s, ' * len(LINE_DATA_FIELDS[line_type])
    VALUE_SLOTS = VALUE_SLOTS[:-2]

    line_table = LINE_TABLES[UC_LINE_TYPES.index(line_type)]

    i_line = "INSERT INTO " + line_table + " (" + ",".join(LINE_DATA_FIELDS[line_type]) + \
               ") VALUES (" + VALUE_SLOTS + ")"
    LINE_DATA_Q[line_type] = i_line

    if line_type in VAR_LINE_TYPES:

        VALUE_SLOTS = '%s, ' * len(LINE_DATA_VAR_FIELDS[line_type])
        VALUE_SLOTS = VALUE_SLOTS[:-2]

        var_line_table = LINE_DATA_VAR_TABLES[line_type]

        i_line = "INSERT INTO " + var_line_table + " (" + \
                 ",".join(LINE_DATA_VAR_FIELDS[line_type]) + \
                 ") VALUES (" + VALUE_SLOTS + ")"
        LINE_DATA_VAR_Q[line_type] = i_line

LINE_DATA_COLS[PERC] = LINE_DATA_COLS[PERC][0:-2] + [FCST_PERC, OBS_PERC]

# column name of n_* after Total. phist, orank, and eclv have extra fields after Total
LINE_VAR_COUNTER[PCT] = '1'
LINE_VAR_COUNTER[PSTD] = '1'
LINE_VAR_COUNTER[PJC] = '1'
LINE_VAR_COUNTER[PRC] = '1'
LINE_VAR_COUNTER[MCTC] = '1'
LINE_VAR_COUNTER[RHIST] = '1'
LINE_VAR_COUNTER[RELP] = '1'
LINE_VAR_COUNTER[PHIST] = '2'
LINE_VAR_COUNTER[ORANK] = '11'
LINE_VAR_COUNTER[ECLV] = '3'

# how many variables/fields appear after i_value (length of repeat)
LINE_VAR_REPEATS[PCT] = 3
LINE_VAR_REPEATS[PSTD] = 1
LINE_VAR_REPEATS[PJC] = 7
LINE_VAR_REPEATS[PRC] = 3
LINE_VAR_REPEATS[MCTC] = 1
LINE_VAR_REPEATS[RHIST] = 1
LINE_VAR_REPEATS[RELP] = 1
LINE_VAR_REPEATS[PHIST] = 1
LINE_VAR_REPEATS[ORANK] = 1
LINE_VAR_REPEATS[ECLV] = 2

RHIST_OLD = ['V7.0', 'V6.1', 'V6.0', 'V5.2', 'V5.1', 'V5.0',
             'V4.2', 'V4.1', 'V4.0', 'V3.1', 'V3.0']

RHIST_5 = ['V5.2', 'V5.1']
RHIST_6 = ['V6.1', 'V6.0', 'V7.0']

X_POINTS_ECON = [0.952380952, 0.909090909, 0.800000000, 0.666666667, 0.500000000,
                 0.333333333, 0.200000000, 0.125000000, 0.100000000, 0.055555556,
                 0.037037037, 0.025000000, 0.016666667, 0.011111111, 0.007142857,
                 0.004761905, 0.002857143, 0.002000000]

# From the data_file_lu table - to lookup file type
POINT_STAT = 0
GRID_STAT = 1
MODE_CTS = 2
MODE_OBJ = 3
WAVELET_STAT = 4
ENSEMBLE_STAT = 5
VSDB_POINT_STAT = 6
STAT = 7
MTD_2D = 8
MTD_3D_PC = 9
MTD_3D_PS = 10
MTD_3D_SC = 11
MTD_3D_SS = 12

# mode file fields
MODE_HEADER = 'mode_header'
MODE_HEADER_ID = 'mode_header_id'
MODE_CTS_T = 'mode_cts'
LINE_TYPE_LU_ID = 'line_type_lu_id'
LINENUMBER = 'linenumber'
FCST_VALID = 'fcst_valid'
FCST_ACCUM = 'fcst_accum'
FCST_RAD = 'fcst_rad'
FCST_THR = 'fcst_thr'
FCST_INIT = 'fcst_init'
OBS_VALID = 'obs_valid'
OBS_ACCUM = 'obs_accum'
OBS_RAD = 'obs_rad'
OBS_THR = 'obs_thr'
OBJECT_ID = 'object_id'
N_VALID = 'n_valid'
GRID_RES = 'grid_res'

MODE_HEADER_KEYS = [VERSION, MODEL, N_VALID, GRID_RES, DESCR,
                    FCST_LEAD, FCST_VALID, FCST_ACCUM, FCST_INIT,
                    OBS_LEAD, OBS_VALID, OBS_ACCUM, FCST_RAD, FCST_THR,
                    OBS_RAD, OBS_THR, FCST_VAR, FCST_UNITS, FCST_LEV,
                    OBS_VAR, OBS_UNITS, OBS_LEV]

MODE_HEADER_FIELDS = [MODE_HEADER_ID, LINE_TYPE_LU_ID, DATA_FILE_ID, LINENUMBER,
                      VERSION, MODEL, N_VALID, GRID_RES, DESCR,
                      FCST_LEAD, FCST_VALID, FCST_ACCUM, FCST_INIT,
                      OBS_LEAD, OBS_VALID, OBS_ACCUM, FCST_RAD, FCST_THR,
                      OBS_RAD, OBS_THR, FCST_VAR, FCST_UNITS, FCST_LEV,
                      OBS_VAR, OBS_UNITS, OBS_LEV]

MODE_CTS_FIELDS = [MODE_HEADER_ID, 'field', TOTAL_LC, FY_OY, FY_ON, FN_OY, FN_ON,
                   'baser', 'fmean', 'acc', 'fbias', 'pody', 'podn', 'pofd',
                   'far', 'csi', 'gss', 'hk', 'hss', 'odds']

Q_MHEADER = "SELECT mode_header_id FROM mode_header WHERE " + \
           "=%s AND ".join(MODE_HEADER_KEYS) + "=%s"

INS_MHEADER = "INSERT INTO mode_header (" + ",".join(MODE_HEADER_FIELDS) + \
             ") VALUES (" + VALUE_SLOTS + ")"

C_VALUE_SLOTS = '%s, ' * len(MODE_CTS_FIELDS)
C_VALUE_SLOTS = C_VALUE_SLOTS[:-2]

INS_CHEADER = "INSERT INTO mode_cts (" + ",".join(MODE_CTS_FIELDS) + \
              ") VALUES (" + C_VALUE_SLOTS + ")"
