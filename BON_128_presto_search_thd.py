#!/usr/bin/env python
import glob, os, os.path, shutil, socket, struct, sys, time
sys.path.append("/usr/local/presto/lib/python")
sys.path.append("/usr/local/presto")
sys.path.append("/usr/local/pgplot")
sys.path.append("/usr/local/tempo")

os.system('echo $PYTHONPATH')
os.environ['PATH'] = "/usr/bin:/usr/sbin:/sbin/:/bin:/usr/local/bin/:/usr/local/presto/bin"
import numpy, psr_utils, presto, sifting, sigproc

# Calling convention:
#
# BON_presto_search.py fil_file working_dir
#
#   fil_file is the filterbank file name
#   working_dir is the scratch directory where the work should be done
#       In general, there should be ~30GB of scratch disk per beam.
#       If you do not have that much scratch space, you will likely
#       need to to use set the use_subbands flag below.

# Basic parameters
# institution is one of: 'UBC', 'NRAOCV', 'McGill', 'Columbia', 'Cornell', 'UTB'
institution           = "NRAOCV" 
base_output_directory = "/data/BON/results/"
db_pointing_file      = "/home/sransom/results/ALFA/PALFA_coords_table.txt"
default_zaplist = "/data/BON/BON.zaplist"

# The following determines if we'll dedisperse and fold using subbands.
# In general, it is a very good idea to use them if there is enough scratch
# space on the machines that are processing (~30GB/beam processed)
#use_subbands          = True
use_subbands          = False 

# Tunable parameters for searching and folding
# (you probably don't need to tune any of them)
rfifind_chunk_time    = 0.8  # ~2.1 sec for dt = 64us 
singlepulse_threshold = 5.0  # threshold SNR for candidate determination
singlepulse_plot_SNR  = 6.0  # threshold SNR for singlepulse plot
singlepulse_maxwidth  = 0.7  # max pulse width in seconds
to_prepfold_sigma     = 5.0  # incoherent sum significance to fold candidates
max_cands_to_fold     = 150  # Never fold more than this many candidates
numhits_to_fold       = 2    # Number of DMs with a detection needed to fold
low_DM_cutoff         = 1.0  # Lowest DM to consider as a "real" pulsar
lo_accel_numharm      = 8    # max harmonics
lo_accel_sigma        = 3.0  # threshold gaussian significance
lo_accel_zmax         = 200    # bins
lo_accel_flo          = 2.0  # Hz
hi_accel_numharm      = 8    # max harmonics
hi_accel_sigma        = 3.0  # threshold gaussian significance
hi_accel_zmax         = 50   # bins
hi_accel_flo          = 1.0  # Hz
low_T_to_search       = 20.0 # sec
max_masked_fraction   = 0.4  # Maximum masked fraction 40% to search for

# Sifting specific parameters (don't touch without good reason!)
sifting.sigma_threshold = to_prepfold_sigma-1.0  # incoherent power threshold (sigma)
sifting.c_pow_threshold = 100.0                  # coherent power threshold
sifting.r_err           = 1.1    # Fourier bin tolerence for candidate equivalence
sifting.short_period    = 0.0007 # Shortest period candidates to consider (s)
sifting.long_period     = 15.0   # Longest period candidates to consider (s)
sifting.harm_pow_cutoff = 8.0    # Power required in at least one harmonic

# A list of 4-digit numbers that are highly factorable by small primes
#goodfactors = [int(x) for x in open("ALFA_goodfacts.txt")]
goodfactors = [1008, 1024, 1056, 1120, 1152, 1200, 1232, 1280, 1296,
               1344, 1408, 1440, 1536, 1568, 1584, 1600, 1680, 1728,
               1760, 1792, 1920, 1936, 2000, 2016, 2048, 2112, 2160,
               2240, 2304, 2352, 2400, 2464, 2560, 2592, 2640, 2688,
               2800, 2816, 2880, 3024, 3072, 3136, 3168, 3200, 3360,
               3456, 3520, 3584, 3600, 3696, 3840, 3872, 3888, 3920,
               4000, 4032, 4096, 4224, 4320, 4400, 4480, 4608, 4704,
               4752, 4800, 4928, 5040, 5120, 5184, 5280, 5376, 5488,
               5600, 5632, 5760, 5808, 6000, 6048, 6144, 6160, 6272,
               6336, 6400, 6480, 6720, 6912, 7040, 7056, 7168, 7200,
               7392, 7680, 7744, 7776, 7840, 7920, 8000, 8064, 8192,
               8400, 8448, 8624, 8640, 8800, 8960, 9072, 9216, 9408,
               9504, 9600, 9680, 9856]

def choose_N(orig_N):
    """
    choose_N(orig_N):
        Choose a time series length that is larger than
            the input value but that is highly factorable.
            Note that the returned value must be divisible
            by at least the maximum downsample factor * 2.
            Currently, this is 8 * 2 = 16.
    """
    if orig_N < 10000:
        return 0
    # Get the number represented by the first 4 digits of orig_N
    first4 = int(str(orig_N)[:4])
    # Now get the number that is just bigger than orig_N
    # that has its first 4 digits equal to "factor"
    for factor in goodfactors:
        if factor > first4: break
    new_N = factor
    while new_N < orig_N:
        new_N *= 10
    # Finally, compare new_N to the closest power_of_two
    # greater than orig_N.  Take the closest.
    two_N = 2
    while two_N < orig_N:
        two_N *= 2
    if two_N < new_N: return two_N
    else: return new_N

def get_baryv(ra, dec, mjd, T, obs="NC"):
    """
    get_baryv(ra, dec, mjd, T):
         Determine the average barycentric velocity towards 'ra', 'dec'
             during an observation from 'obs'.  The RA and DEC are in the
             standard string format (i.e. 'hh:mm:ss.ssss' and
             'dd:mm:ss.ssss').  'T' is in sec and 'mjd' is (of course) in MJD.
   """
    tts = psr_utils.span(mjd, mjd+T/86400.0, 100)
    nn = len(tts)
    bts = numpy.zeros(nn, dtype=numpy.float64)
    vel = numpy.zeros(nn, dtype=numpy.float64)
    presto.barycenter(tts, bts, vel, nn, ra, dec, obs, "DE200")
    avgvel = numpy.add.reduce(vel)/nn
    return avgvel

def fix_fil_posn(fil_filenm, hdrlen, ra, dec):
    """
    fix_fil_posn(fil_filenm, hdrlen, ra, dec):
        Modify the filterbank header and update the RA and DEC
            fields using the values given as input.  ra and dec
            should be in 'HH:MM:SS.SSSS' and 'DD:MM:SS.SSSS' format.
            hdrlen is the length of the filterbank header as
            reported by PRESTO's 'readfile' or SIGPROC's 'header'.
    """
    newra = float(ra.replace(":", ""))
    newdec = float(dec.replace(":", ""))
    header = open(fil_filenm).read(hdrlen)
    ra_ptr = header.find("src_raj")+len("src_raj")
    dec_ptr = header.find("src_dej")+len("src_dej")
    filfile = open(fil_filenm, 'rb+')
    filfile.seek(ra_ptr)
    filfile.write(struct.pack('d', newra))
    filfile.seek(dec_ptr)
    filfile.write(struct.pack('d', newdec))
    filfile.close()

def read_db_posn(orig_filenm, beam):
    """
    read_db_posn(orig_filenm, beam):
        Find the original WAPP filename in the db_pointing_file
           and return the sexagesimal position strings for
           the choen beam in that file.  Return None if not found.
    """
    offset = beam % 2
    for line in open(db_pointing_file):
        sline = line.split()
        if sline[0].strip() == orig_filenm:
            ra_str = sline[2*offset+1].strip()
            dec_str = sline[2*offset+2].strip()
            return ra_str, dec_str
    return None

def find_masked_fraction(obs):
    """
    find_masked_fraction(obs):
        Parse the output file from an rfifind run and return the
            fraction of the data that was suggested to be masked.
    """
    rfifind_out = obs.basefilenm + "_rfifind.out"
    for line in open(rfifind_out):
        if "Number of  bad   intervals" in line:
            return float(line.split("(")[1].split("%")[0])/100.0
    # If there is a problem reading the file, return 100%
    return 100.0

def get_all_subdms(ddplans):
    """
    get_all_subdms(ddplans):
        Return a sorted array of the subdms from the list of ddplans.
    """
    subdmlist = []
    for ddplan in ddplans:
        subdmlist += [float(x) for x in ddplan.subdmlist]
    subdmlist.sort()
    subdmlist = numpy.asarray(subdmlist)
    return subdmlist

def find_closest_subbands(obs, subdms, DM):
    """
    find_closest_subbands(obs, subdms, DM):
        Return the basename of the closest set of subbands to DM
        given an obs_info class and a sorted array of the subdms.
    """
    subdm = subdms[numpy.fabs(subdms - DM).argmin()]
    return "subbands/%s_DM%.2f.sub[0-6]*"%(obs.basefilenm, subdm)

def timed_execute(cmd): 
    """
    timed_execute(cmd):
        Execute the command 'cmd' after logging the command
            to STDOUT.  Return the wall-clock amount of time
            the command took to execute.
    """
    sys.stdout.write("\n'"+cmd+"'\n")
    sys.stdout.flush()
    start = time.time()
    os.system(cmd)
    end = time.time()
    return end - start

def count_sp(filename):
    pfi = open(filename,'r')
    number_sp=0
    for line in pfi:
      number_sp += 1
    pfi.close()
    return number_sp

def get_folding_command(cand, obs, ddplans, maskfilenm):
    """
    get_folding_command(cand, obs, ddplans):
        Return a command for prepfold for folding the subbands using
            an obs_info instance, a list of the ddplans, and a candidate 
            instance that describes the observations and searches.
    """
    # Folding rules are based on the facts that we want:
    #   1.  Between 24 and 200 bins in the profiles
    #   2.  For most candidates, we want to search length = 101 p/pd/DM cubes
    #       (The side of the cube is always 2*M*N+1 where M is the "factor",
    #       either -npfact (for p and pd) or -ndmfact, and N is the number of bins
    #       in the profile).  A search of 101^3 points is pretty fast.
    #   3.  For slow pulsars (where N=100 or 200), since we'll have to search
    #       many points, we'll use fewer intervals in time (-npart 30)
    #   4.  For the slowest pulsars, in order to avoid RFI, we'll
    #       not search in period-derivative.
    zmax = cand.filename.split("_")[-1]
    outfilenm = obs.basefilenm+"_DM%s"%(cand.DMstr)

    # Note:  the following calculations should probably only be done once,
    #        but in general, these calculation are effectively instantaneous
    #        compared to the folding itself
    if use_subbands:  # Fold the subbands
        subdms = get_all_subdms(ddplans)
        subfiles = find_closest_subbands(obs, subdms, cand.DM)
        foldfiles = subfiles
    else:  # Folding the downsampled filterbank files instead
        hidms = [x.lodm for x in ddplans[1:]] + [2000]
        dfacts = [x.downsamp for x in ddplans]
        for hidm, dfact in zip(hidms, dfacts):
            if cand.DM < hidm:
                downsamp = dfact
                break
        #if downsamp==1:
        #    filfile = obs.fil_filenm
        #else:
        #    filfile = obs.basefilenm+"_DS%d.fil"%downsamp
        filfile = obs.fil_filenm

        foldfiles = filfile
    p = 1.0 / cand.f
    if p < 0.002:
        Mp, Mdm, N = 2, 2, 24
        otheropts = "-npart 50 "
    elif p < 0.05:
        Mp, Mdm, N = 2, 2, 50
        otheropts = "-npart 40 -pstep 1 -dmstep 3"
    elif p < 0.5:
        Mp, Mdm, N = 1, 1, 100
        otheropts = "-npart 30 -pstep 1 -dmstep 1"
    else:
        Mp, Mdm, N = 1, 1, 100
        otheropts = "-npart 30 -pstep 1 -dmstep 1"
    return "prepfold -filterbank -noxwin -nopdsearch -mask %s -accelcand %d -accelfile %s.cand -dm %.2f -o %s %s -n %d -npfact %d -ndmfact %d %s > /dev/null" % \
           (maskfilenm, cand.candnum, cand.filename, cand.DM, outfilenm,
            otheropts, N, Mp, Mdm, foldfiles)

class obs_info:
    """
    class obs_info(fil_filenm,box)
        A class describing the observation and the analysis.
    """
    def __init__(self, fil_filenm, box, workdir):
        self.fil_filenm = fil_filenm
        self.basefilenm = fil_filenm.rstrip(".fbk")
        #self.beam = int(self.basefilenm[-1])
        self.beam = int(self.basefilenm)
        filhdr, self.hdrlen = sigproc.read_header(fil_filenm)
        self.orig_filenm = filhdr['rawdatafile']
        self.MJD = filhdr['tstart']
        self.nchans = filhdr['nchans']
        self.ra_rad = sigproc.ra2radians(filhdr['src_raj'])
        self.ra_string = psr_utils.coord_to_string(\
            *psr_utils.rad_to_hms(self.ra_rad))
        self.dec_rad = sigproc.dec2radians(filhdr['src_dej'])
        self.dec_string = psr_utils.coord_to_string(\
            *psr_utils.rad_to_dms(self.dec_rad))
        self.az = filhdr['az_start']
        self.el = 90.0-filhdr['za_start']
	self.fch1 = filhdr['fch1']
        self.BW = abs(filhdr['foff']) * filhdr['nchans']
        self.dt = filhdr['tsamp']
        self.orig_N = sigproc.samples_per_file(fil_filenm, filhdr, self.hdrlen)
        self.orig_T = self.orig_N * self.dt
        self.N = choose_N(self.orig_N)
        #self.N = 2097152 
        self.T = self.N * self.dt
        # Update the RA and DEC from the database file if required
        #newposn = read_db_posn(self.orig_filenm, self.beam)
        #if newposn is not None:
        #    self.ra_string, self.dec_string = newposn
            # ... and use them to update the filterbank file
        #    fix_fil_posn(fil_filenm, self.hdrlen,
        #                 self.ra_string, self.dec_string)
        # Determine the average barycentric velocity of the observation
        self.baryv = get_baryv(self.ra_string, self.dec_string,
                               self.MJD, self.T, obs="NC")
        # Where to dump all the results
        # Directory structure is under the base_output_directory
        # according to base/MJD/filenmbase/beam
        self.outputdir = os.path.join(base_output_directory,box,
                                      self.basefilenm)
	self.workdir = workdir
        # Figure out which host we are processing on
        self.hostname = socket.gethostname()
        # The fraction of the data recommended to be masked by rfifind
        self.masked_fraction = 0.0
        # Initialize our timers
        self.rfifind_time = 0.0
        self.downsample_time = 0.0
        self.subbanding_time = 0.0
        self.dedispersing_time = 0.0
        self.FFT_time = 0.0
        self.lo_accelsearch_time = 0.0
        self.hi_accelsearch_time = 0.0
        self.singlepulse_time = 0.0
        self.sifting_time = 0.0
        self.folding_time = 0.0
        self.total_time = 0.0
        # Inialize some candidate counters
        self.num_sifted_cands = 0
        self.num_folded_cands = 0
        self.num_single_cands = 0
	self.nb_sp = 0
        self.dmstrs = []
	self.all_accel_cands = []

    def write_report(self, filenm):
        report_file = open(filenm, "w")
        report_file.write("---------------------------------------------------------\n")
        report_file.write("%s was processed on %s\n"%(self.fil_filenm, self.hostname))
        report_file.write("Ending UTC time:  %s\n"%(time.asctime(time.gmtime())))
        report_file.write("Total wall time:  %.1f s (%.2f hrs)\n"%\
                          (self.total_time, self.total_time/3600.0))
        report_file.write("Fraction of data masked:  %.2f%%\n"%\
                          (self.masked_fraction*100.0))
	#report_file.write("Number of SinglePulses detected:  %d\n"%self.nb_sp)
        report_file.write("---------------------------------------------------------\n")
        report_file.write("          rfifind time = %7.1f sec (%5.2f%%)\n"%\
                          (self.rfifind_time, self.rfifind_time/self.total_time*100.0))
        if use_subbands:
            report_file.write("       subbanding time = %7.1f sec (%5.2f%%)\n"%\
                              (self.subbanding_time, self.subbanding_time/self.total_time*100.0))
        else:
            report_file.write("     downsampling time = %7.1f sec (%5.2f%%)\n"%\
                              (self.downsample_time, self.downsample_time/self.total_time*100.0))
        report_file.write("     dedispersing time = %7.1f sec (%5.2f%%)\n"%\
                          (self.dedispersing_time, self.dedispersing_time/self.total_time*100.0))
        report_file.write("     single-pulse time = %7.1f sec (%5.2f%%)\n"%\
                          (self.singlepulse_time, self.singlepulse_time/self.total_time*100.0))
        report_file.write("              FFT time = %7.1f sec (%5.2f%%)\n"%\
                          (self.FFT_time, self.FFT_time/self.total_time*100.0))
        report_file.write("   lo-accelsearch time = %7.1f sec (%5.2f%%)\n"%\
                          (self.lo_accelsearch_time, self.lo_accelsearch_time/self.total_time*100.0))
        report_file.write("   hi-accelsearch time = %7.1f sec (%5.2f%%)\n"%\
                          (self.hi_accelsearch_time, self.hi_accelsearch_time/self.total_time*100.0))
        report_file.write("          sifting time = %7.1f sec (%5.2f%%)\n"%\
                          (self.sifting_time, self.sifting_time/self.total_time*100.0))
        report_file.write("          folding time = %7.1f sec (%5.2f%%)\n"%\
                          (self.folding_time, self.folding_time/self.total_time*100.0))
        report_file.write("---------------------------------------------------------\n")
        report_file.close()

class dedisp_plan:
    """
    class dedisp_plan(lodm, dmstep, dmsperpass, numpasses, numsub, downsamp)
        A class describing a de-dispersion plan for prepsubband in detail.
    """
    def __init__(self, lodm, dmstep, dmsperpass, numpasses, numsub, downsamp):
        self.lodm = float(lodm)
        self.dmstep = float(dmstep)
        self.dmsperpass = int(dmsperpass)
        self.numpasses = int(numpasses)
        self.numsub = int(numsub)
        self.downsamp = int(downsamp)
        # Downsample less for the subbands so that folding
        # candidates is more accurate
        self.sub_downsamp = self.downsamp / 2
        if self.sub_downsamp==0: self.sub_downsamp = 1
        # The total downsampling is:
        #   self.downsamp = self.sub_downsamp * self.dd_downsamp
        if self.downsamp==1: self.dd_downsamp = 1
        else: self.dd_downsamp = 2
        self.sub_dmstep = self.dmsperpass * self.dmstep
        self.dmlist = []  # These are strings for comparison with filenames
        self.subdmlist = []
        for ii in range(self.numpasses):
            self.subdmlist.append("%.2f"%(self.lodm + (ii+0.5)*self.sub_dmstep))
            lodm = self.lodm + ii * self.sub_dmstep
            dmlist = ["%.2f"%dm for dm in \
                      numpy.arange(self.dmsperpass)*self.dmstep + lodm]
            self.dmlist.append(dmlist)

def preprocess_bon(bon_filenm_long, box):

    # 
    #cpu_idx = int(cpu_id)

    c = bon_filenm_long.rindex("/")
    bon_filenm = bon_filenm_long[c+1:]
    print bon_filenm

    workdir="/data/BON/work/%s"%bon_filenm.rstrip(".fbk")

    # Make sure the temp directory exist
    try:
        os.makedirs(workdir)
    except: pass

    # Change to the specified working directory
    os.chdir(workdir)

    # Copy data
    cmd = "scp -p root@clairvaux:/data?/%s_*.fbk ."%(bon_filenm_long.rstrip(".fbk"))
    print cmd
    os.system(cmd)
   
    # Convert BON file to sigproc format - need .i file
    print "Join BON .fbk files\n"
    cmd = "joinfbk %s_*.fbk"%(bon_filenm.rstrip(".fbk"))
    print cmd
    os.system(cmd)

    fil_filenm = bon_filenm
    #fil_filenm = fil_filenm.rstrip(".fb")+".sig"

    # Get information on the observation and the jbo
    job = obs_info(fil_filenm, box, workdir)

    ddplans = []
    if (job.dt<0.000048 and job.nchans==512):  # BON 32us and 0.25 Mhz or 512 channels over 128MHz band
        # The values here are:       lodm dmstep dms/call #calls #subbands downsamp
	ddplans.append(dedisp_plan(   0.0,   0.2,     105,     1,       512,       1))
	ddplans.append(dedisp_plan(  21.0,   0.2,     105,     1,       512,       1))
        ddplans.append(dedisp_plan(  42.0,   0.2,     220,     1,       512,       1))
	ddplans.append(dedisp_plan(  86.0,   0.2,     221,     1,       512,       1))
	ddplans.append(dedisp_plan( 130.2,   0.3,     189,     1,       512,       1))
	ddplans.append(dedisp_plan( 186.9,   0.3,     190,     1,       512,       1))
	ddplans.append(dedisp_plan( 243.9,   0.5,     189,     1,       512,       2))
	ddplans.append(dedisp_plan( 338.4,   0.5,     189,     1,       512,       2))
	ddplans.append(dedisp_plan( 432.9,   1.0,     211,     1,       512,       4))
	ddplans.append(dedisp_plan( 643.9,   1.0,     210,     1,       512,       8))
	ddplans.append(dedisp_plan( 853.9,   3.0,     130,     1,       512,       8))

    elif (job.dt<0.000048 and job.nchans==256):  # BON 32us and 0.25 Mhz or 256 channels over 64MHz band
        # The values here are:       lodm dmstep dms/call #calls #subbands downsamp
	ddplans.append(dedisp_plan(   0.0,   0.2,     210,     1,       256,       1))
        ddplans.append(dedisp_plan(  42.0,   0.2,     220,     1,       256,       1))
	ddplans.append(dedisp_plan(  86.0,   0.2,     221,     1,       256,       1))
	ddplans.append(dedisp_plan( 130.2,   0.3,     189,     1,       256,       1))
	ddplans.append(dedisp_plan( 186.9,   0.3,     190,     1,       256,       1))
	ddplans.append(dedisp_plan( 243.9,   0.5,     189,     1,       256,       2))
	ddplans.append(dedisp_plan( 338.4,   0.5,     189,     1,       256,       2))
	ddplans.append(dedisp_plan( 432.9,   1.0,     211,     1,       256,       4))
	ddplans.append(dedisp_plan( 643.9,   1.0,     210,     1,       256,       8))
	ddplans.append(dedisp_plan( 853.9,   3.0,     130,     1,       256,       8))

    else: # faster option : for 64us and 1MHz or 128 channels
        # The values here are:       lodm dmstep dms/call #calls #subbands downsamp
        ddplans.append(dedisp_plan(   0.0,   0.3,     212,     1,       128,       1))
        ddplans.append(dedisp_plan(  63.7,   0.5,      75,     1,       128,       1))
        ddplans.append(dedisp_plan( 101.2,   1.0,      89,     1,       128,       2))
        ddplans.append(dedisp_plan( 190.2,   3.0,      81,     1,       128,       4))
        ddplans.append(dedisp_plan( 433.2,   5.0,     155,     1,       128,       8))
    
    # If obs too short
    if job.T < low_T_to_search:
        print "The observation is too short (%.2f s) to search."%job.T
	return -1,job,ddplans

    job.total_time = time.time()
    
    # Make sure the output directory (and parent directories) exist
    try:
        os.makedirs(job.outputdir)
    except: pass

    # Create a directory to hold all the subbands
    if use_subbands:
        try:
            os.makedirs("subbands")
        except: pass
    
    print "\nBeginning BON search of '%s'"%job.fil_filenm
    print "UTC time is:  %s"%(time.asctime(time.gmtime()))

    # rfifind the filterbank file
    cmd = "rfifind -filterbank -time %.17g -o %s %s > %s_rfifind.out"%\
          (rfifind_chunk_time, job.basefilenm,
           job.fil_filenm, job.basefilenm)
    	   
    if (job.dt<0.000048 and job.nchans>=256 and job.fch1<1500.0):  
	   cmd = "rfifind -filterbank -zapchan 16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,68,94,107,108,109 -time %.17g -o %s %s > %s_rfifind.out"%\
	             (rfifind_chunk_time, job.basefilenm,
		                job.fil_filenm, job.basefilenm)
    job.rfifind_time += timed_execute(cmd)
    maskfilenm = job.basefilenm + "_rfifind.mask"
    # Find the fraction that was suggested to be masked
    # Note:  Should we stop processing if the fraction is
    #        above some large value?  Maybe 30%?
    job.masked_fraction = find_masked_fraction(job)
    if job.masked_fraction > max_masked_fraction:
        print "The observation is bad (%.2f%% of data masked)."%(job.masked_fraction*100.0)
	return -2,job,ddplans

    return 0,job,ddplans

def thd_process_bon(plan_id, job, ddplan):
    
    # 
    #cpu_idx = int(cpu_id)
    maskfilenm = job.basefilenm + "_rfifind.mask"


    # Change to the specified working directory
    os.chdir(job.workdir)

    # Iterate over the individual passes through the .fil file
    for passnum in range(ddplan.numpasses):
            subbasenm = "%s_DM%s"%(job.basefilenm, ddplan.subdmlist[passnum])

            cmd = "prepsubband -filterbank -mask %s -lodm %.2f -dmstep %.2f -numdms %d -numout %d -o %s %s -downsamp %d -nsub %d >> /tmp/prepsubband"%\
                  ( maskfilenm, ddplan.lodm+passnum*ddplan.sub_dmstep, ddplan.dmstep,
                   ddplan.dmsperpass, job.N/ddplan.downsamp,
                   job.basefilenm, job.fil_filenm, ddplan.downsamp, ddplan.numsub)
            job.dedispersing_time += timed_execute(cmd)

            # Iterate over all the new DMs
            for dmstr in ddplan.dmlist[passnum]:
                job.dmstrs.append(dmstr)
                basenm = job.basefilenm+"_DM"+dmstr
                datnm = basenm+".dat"
                fftnm = basenm+".fft"
                infnm = basenm+".inf"

                # Do the single-pulse search
                cmd = "single_pulse_search.py -p -t %f %s > /dev/null"%\
                      (singlepulse_threshold, datnm)
                #cmd = "single_pulse_search.py -p -m %f -t %f %s > /dev/null"%\
                #      (cpu_idx, singlepulse_maxwidth, singlepulse_threshold, datnm)
		if os.path.isfile(datnm):
                    job.singlepulse_time += timed_execute(cmd)
                #try:
                #    shutil.copy(basenm+".singlepulse", job.outputdir)
                #except: pass

		#job.nb_sp += count_sp(basenm+".singlepulse")

                # FFT, zap, and de-redden
                #cmd = "realfft %s"%datnm
                #job.FFT_time += timed_execute(cmd)
                #cmd = "zapbirds -zap -zapfile %s -baryv %.6g %s"%\
                #      (default_zaplist, job.baryv, fftnm)
                #job.FFT_time += timed_execute(cmd)
                #cmd = "rednoise %s"%fftnm
                #job.FFT_time += timed_execute(cmd)
                #try:
                #    os.rename(basenm+"_red.fft", fftnm)
                #except: pass
                
                # Do the low-acceleration search
	        cmd = "accelsearch -numharm %d -sigma %f -zmax %d -flo %f %s -zaplist %s -baryv %.6g > /dev/null"%\
                      ( lo_accel_numharm, lo_accel_sigma, lo_accel_zmax, lo_accel_flo, datnm,default_zaplist, job.baryv)
                job.lo_accelsearch_time += timed_execute(cmd)
                
                try:
                    os.remove(basenm+"_ACCEL_%d.txtcand"%lo_accel_zmax)
                except: pass
                
                #try:  # This prevents errors if there are no cand files to copy
                #    shutil.copy(basenm+"_ACCEL_%d.cand"%lo_accel_zmax, job.outputdir)
                #    shutil.copy(basenm+"_ACCEL_%d"%lo_accel_zmax, job.outputdir)
                #except: pass
        
                # Do the high-acceleration search
                #cmd = "accelsearch -numharm %d -sigma %f -zmax %d -flo %f %s"%\
                #      (hi_accel_numharm, hi_accel_sigma, hi_accel_zmax, hi_accel_flo, fftnm)
                #job.hi_accelsearch_time += timed_execute(cmd)
                #try:
                #    os.remove(basenm+"_ACCEL_%d.txtcand"%hi_accel_zmax)
                #except: pass
                #try:  # This prevents errors if there are no cand files to copy
                #    shutil.copy(basenm+"_ACCEL_%d.cand"%hi_accel_zmax, job.outputdir)
                #    shutil.copy(basenm+"_ACCEL_%d"%hi_accel_zmax, job.outputdir)
                #except: pass

                # Remove the .dat and .fft files
                try:
                    os.remove(datnm)
                    os.remove(fftnm)
                except: pass

def sift_process_bon(job):

    # 
    #cpu_idx = int(cpu_id)

    # Change to the specified working directory
    os.chdir(job.workdir)

    # Make the single-pulse plot
    cmd = "single_pulse_search.py -t %f %s/*.singlepulse"%(singlepulse_plot_SNR,job.workdir)
    job.singlepulse_time += timed_execute(cmd)

    # Sort dmstrs
    job.dmstrs.sort()

    # Sift through the candidates to choose the best to fold
    
    job.sifting_time = time.time()

    lo_accel_cands = sifting.read_candidates(glob.glob("%s/*ACCEL_%d"%(job.workdir,lo_accel_zmax)))
    if len(lo_accel_cands):
        lo_accel_cands = sifting.remove_duplicate_candidates(lo_accel_cands)
    if len(lo_accel_cands):
        lo_accel_cands = sifting.remove_DM_problems(lo_accel_cands, numhits_to_fold,
                                                    job.dmstrs, low_DM_cutoff)
#
#    hi_accel_cands = sifting.read_candidates(glob.glob("*ACCEL_%d"%hi_accel_zmax))
#    if len(hi_accel_cands):
#        hi_accel_cands = sifting.remove_duplicate_candidates(hi_accel_cands)
#    if len(hi_accel_cands):
#        hi_accel_cands = sifting.remove_DM_problems(hi_accel_cands, numhits_to_fold,
#                                                    dmstrs, low_DM_cutoff)

    job.all_accel_cands = lo_accel_cands #+ hi_accel_cands
    if len(job.all_accel_cands):
        job.all_accel_cands = sifting.remove_harmonics(job.all_accel_cands)
        # Note:  the candidates will be sorted in _sigma_ order, not _SNR_!
        job.all_accel_cands.sort(sifting.cmp_sigma)
        sifting.write_candlist(job.all_accel_cands, job.basefilenm+".accelcands")

    try:
        cmd = "cp %s/*.accelcands "%job.workdir +job.outputdir
        os.system(cmd)
    except: pass

    job.sifting_time = time.time() - job.sifting_time


def fold_process_bon(cand, job, ddplans):

    # 
    #cpu_idx = int(cpu_id)
    maskfilenm = job.basefilenm + "_rfifind.mask"

    # Change to the specified working directory
    os.chdir(job.workdir)

    # Fold the best candidates
    if cand.sigma > to_prepfold_sigma:
        job.folding_time += timed_execute(get_folding_command(cand, job, ddplans, maskfilenm))

def compress_process_bon(job,box):
    # Now step through the .ps files and convert them to .png and gzip them

    # 
    #cpu_idx = int(cpu_id)

    # Change to the specified working directory
    os.chdir(job.workdir)

    psfiles = glob.glob("%s/*.ps"%(job.workdir))
    for psfile in psfiles:
        if "singlepulse" in psfile:
            # For some reason the singlepulse files don't transform nicely...
   #         epsfile = psfile.replace(".ps", ".eps")
   #         os.system("eps2eps "+psfile+" "+epsfile)
            os.system("pstoimg -density 300 -flip cw -scale 0.9 -geometry 640x480 " +psfile)
   #         try:
   #             os.remove(epsfile)
   #         except: pass
        else:
            os.system("pstoimg -density 100 -flip cw -scale 0.9 -geometry 640x480 " +psfile)
         #os.system("gzip "+psfile)
    
    # Compress Single-pulse data
    cmd = "tar cfj %s_sp.tar.bz2 *.singlepulse *_singlepulse.ps *.inf "%job.basefilenm
    os.system(cmd)

    # Compress Cands data
    cmd = "tar cfj %s_accel.tar.bz2 *ACCEL_%d"%(job.basefilenm,lo_accel_zmax)
    os.system(cmd)
    cmd = "tar cfj %s_accel_bin.tar.bz2 *ACCEL_%d.cand"%(job.basefilenm,lo_accel_zmax)
    os.system(cmd)

    # Compress RFI data
    cmd = "tar cfj %s_rfifind.tar.bz2 %s_rfifind*"%(job.basefilenm,job.basefilenm)
    os.system(cmd)

    # Compress Candidates PostScript 
    cmd = "tar cfj %s_pfd_ps.tar.bz2 *pfd.ps"%job.basefilenm
    os.system(cmd)

    # Compress Candidates pfd 
    cmd = "tar cfj %s_pfd.tar.bz2 *.pfd"%job.basefilenm
    os.system(cmd)

    # NOTE:  need to add database commands

    # And finish up

    job.total_time = time.time() - job.total_time
    print "\nFinished"
    print "UTC time is:  %s"%(time.asctime(time.gmtime()))

    # Write the job report

    job.write_report(job.basefilenm+".report")
    job.write_report(os.path.join(job.outputdir, job.basefilenm+".report"))

    # Copy all the important stuff to the output directory
    try:
        cmd = "cp *.png *.bz2 "+job.outputdir # Not enough space to save .pfd file
        #cmd = "cp *rfifind.[bimors]* *.pfd *.ps.gz *.png "+job.outputdir
        os.system(cmd)
    except: pass
    
    # Send results to clairvaux
    cmd = "scp -rp %s root@clairvaux:/data6/results/%s/."%(job.outputdir,box)
    print cmd
    os.system(cmd)
    
    # Remove all temporary stuff
    cmd = "cd; rm -rf %s"%job.workdir
    #os.system(cmd)
    
    # Return nb of cands found
    return len(job.all_accel_cands)
