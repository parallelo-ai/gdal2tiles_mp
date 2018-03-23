import shlex
import subprocess
import logging
import datetime
import argparse
import signal

# Dictionary to hold spawned processes
spawned_processes = dict()

class GDAL2TilesSpawner():
    """
    Spawns gdal2tiles tiler processes with subprocess and reports the progress
    through callback functions
    """
    def __repr__(self):
       return "GDAL2TilesSpawner(image=" + self.image + ")"

    def signal_handler(self, signum, frame):
        # Kills child process
        self.kill_process()

    def __init__(self,
                 layer_id,
                 image,
                 profile="mercator",
                 zoom="15-22",
                 alpha="0,0,0",
                 progress_callback=None,
                 done_callback=None,
                 timeout=1800,
                 binary="gdal2tiles_mp.py",
                 python_bin="python3",
                 **kwargs):
        """
        __init__ populates the class members. The method options are,  

        layer_id:(int) layer id of the image to be processed.  
        image:(string) full path to the image.  
        profile:(string) the projection profile.(default "mercator")(options:
        "geodetic" "mercator").  
        zoom:(string) the zoom range, as "10", or "15-22".(default "12-22").  
        alpha:(string) value for the alpha layer.(default "0,0,0")  
        progress_callback:(function) a function that receives a single argument.
                          the purpose is to report the progress of the tiling.
                          (default None)
        done_callback:(function) a function that receives a single argument.
                          the purpose is to report the "Done" status of the
                          tiler.(default None)
        timeout:(int) time in seconds to wait before killing the spawned process
                useful for garbage collecting stalled processes.(default 1800
                seconds)
        binary:(string) name for the gdal2tiles converter. This allows the use
        of diferent gdal2tiles binaries.(default "gdal2tiles_mp.py" which is
        ours)
        python_bin:(string) name for the python binary.(default "python3")

        The options are used to call and raise gdal2tiles_mp.py script through
        subprocess.   

        """
        self.layer_id = layer_id 
        self.image = image
        self.profile = profile
        self.alpha = alpha
        self.progress_callback = progress_callback # progress_callback function
        self.done_callback = done_callback # done_callback function

        self.timeout = timeout
        self.binary = binary
        self.python_bin = python_bin

        if kwargs:
          self.kwargs = kwargs

        # Check the zoom range
        if zoom.count("-") > 0:
            self.zoom_min, self.zoom_max = zoom.split("-")
        else:
            # min and max zoom levels are the same
            self.zoom_min = self.zoom_max = zoom

        # Config log file
        self.init_log()
        self.mk_log_decoration()

        # Signals that i'll listen to
        signal.signal(signal.SIGINT,  self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def init_log(self):
        log_filename = self.image.split(".")[-2] + "-" + datetime.datetime.now().strftime("%y-%m-%d_%H.%M.%S") + "-G2T.log"
        logging.basicConfig(filename=log_filename,format='%(asctime)s - %(levelname)s: %(message)s', level=logging.DEBUG)

    def mk_log_decoration(self):
        logging.info("****************************************")

    def mk_log(self):
        logging.info("GDAL2TilesSpawner PROCESS SPAWNED")
        logging.info("pid: " + str(self.process.pid))
        logging.info("layer_id" + str(self.layer_id))
        logging.info("image: " + self.image)
        logging.info("profile: " + self.profile)
        logging.info("alpha: " + str(self.alpha)) # alpha should be a tuple
        logging.info("timeout: " + str(self.timeout))
        logging.info("command: " + str(self.arglist))
   
    def mk_args(self):
        argstr = self.python_bin
        argstr += " " + self.binary 
        argstr +=  " --profile="+ self.profile
        argstr +=  " -a " + self.alpha
        
        # Process the zooms for arguments
        if self.zoom_max != self.zoom_min:
            argstr +=  " --zoom " + self.zoom_min + "-" + self.zoom_max
        else:
            argstr +=  " --zoom " + self.zoom_min

        # Don't generate html files
        argstr +=  " -w none"
        argstr +=  " " + self.image

        if self.kwargs:
            if 'output' in self.kwargs:
                argstr += " " 
                argstr += self.kwargs["output"]

        # Generate lexically valid argument list
        self.arglist = shlex.split(argstr)

    def __call__(self):
        """
        __call__ executes the class instance as a callable object
        """
        self.mk_args() # Prepare arguments

        # print("Preparing arguments: ",self.arglist)
        self.process = subprocess.Popen(self.arglist, stdout=subprocess.PIPE) # Spawn process 

        spawned_processes[self.process.pid] = {"pid": self.process.pid,
                                               "layer_id": self.layer_id,
                                               "image": self.image,
                                               "profile" : self.profile,
                                               "alpha" : str(self.alpha), # alpha should be a tuple
                                               "timeout" : self.timeout,
                                               "command" : self.arglist,
                                               "progress" : 0}
        # Each dot corresponds to a 2.5% in progress
        step = 2.5
        total = 0.0
        
        # The combination of and b provide a smooth transition
        # between the progress bars of generate_base_tiles and
        # generate_overview_tiles processes
        def percent(x, a=0.85, b=0.15):
            if x <= 100:
                return x*a
            else:
                return 100*a + (x-100)*b

        import re

        # TODO safely open with `with .. as ...`
        for c in iter(lambda: self.process.stdout.read(1),''):
            if c == b'':
                break
            d = c.decode("utf-8")
            if not re.search(r'[^a-zA-Z0\s\-\:]',d):
                continue
            total += step

            # Updates dictionary with the progress of the current process
            spawned_processes[self.process.pid]["progress"] = percent(total)

            if self.progress_callback != None:
                # Pass the percented total to the progress_callback function
                self.progress_callback(percent(total))

        if self.done_callback != None:
            # Pass the percented total to the progress_callback function
            self.done_callback(self.layer_id)
       
        # Log the event
        self.mk_log()

        res = self.process.wait(timeout=self.timeout)

        if res < 0:
            logging.warning("process with pid " + str(self.process.pid) + "terminated with exit code "+ str(res))
            logging.warning("If you didn't killed the process it's very likely that something went wrong!")
        else:
            logging.info("process with pid " + str(self.process.pid) + " terminated successfully")

        # Delete the spawned process from the dictionary
        del spawned_processes[self.process.pid]

        return res 

    def kill_process(self):
        res = None

        # Try to terminate spawned processes, or report the error into the log
        try:
            logging.warning("Terminating child process pid " + str(self.process.pid))
            res = self.process.terminate()
            logging.warning("Child process pid " + str(self.process.pid) + " killed")
        except:
            logging.warning("Terminating child process pid " +
                    str(self.process.pid) + " failed.")
            logging.warning(res)

    def put_log(self,text):
        logging.info(text)

###############################################################################

def main(layer_id, image, profile, zoom, alpha, timeout=1800):
    G2T = GDAL2TilesSpawner(layer_id, image, profile, zoom, alpha, timeout)

    # If it can't run, kill all the spawned processes
    try:
        G2T()
    except:
        G2T.kill_process()


###############################################################################

if __name__ == "__main__":
    ## Instantiate the parser of arguments
    parser = argparse.ArgumentParser(description="Spawn GDAL2Tiles processes")

    ## Add the list of arguments
    parser.add_argument('-l', '--layer_id', help="Layer ID of the image processed", type=str)
    parser.add_argument('-i', '--image', help="Path to the image where the plants will be counted.", type=str)
    parser.add_argument('-t', '--timeout', help="tolerance for the color detection.", type=float)
    parser.add_argument('-a', '--alpha', help="Circle specification [center_x,center_y,radious].", type=str)
    parser.add_argument('-z', '--zoom', help="Circle specification [center_x,center_y,radious].", type=str)
    parser.add_argument('-p', '--profile', help="Plant profile to be loaded")

    ## Parse the input arguments 
    args = parser.parse_args()

    # Check that the arguments passed in good shape
    if not None in [args.layer_id, args.image, args.zoom, args.profile, args.alpha]:
        main(args.layer_id, args.image,  args.profile, args.zoom, args.alpha)
    else:
        print("Wrong set of parameters")
