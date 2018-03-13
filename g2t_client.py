import shlex
import subprocess
import logging
import datetime
import argparse
import signal

# Dictionary to hold spawned processes
spawned_processes = dict()

class GDAL2TilesSpawner():
    def __repr__(self):
       return "GDAL2TilesSpawner(image=" + self.image + ")"

    def signal_handler(self, signum, frame):
        # print("got a signal mate",self, signum, frame)
        # Kills child process
        self.kill_process()

    def __init__(self, image, profile="mercator", zoom="14-22", alpha=(0,0,0),
        timeout=1800 ,binary="gdal2tiles_mp.py", python_bin="python3"):
        """
        __init__ populates the class members. The method options are,  

        image:(string) full path to the image.  
        profile:(string) the projection profile.(default "mercator")(options:
        "geodetic" "mercator").  
        zoom:(string) the zoom range, as "10", or "15-22".(default "12-22").  
        alpha:(tuple)(int) value for the alpha layer.(default (0,0,0))  

        The options are used to call and raise gdal2tiles_mp.py script through
        subprocess.   

        """
        self.image = image
        self.profile = profile
        self.alpha = str(alpha) # alpha should be a tuple
        self.timeout = timeout
        self.binary = binary
        self.python_bin = python_bin

        # Check the zoom range
        if zoom.count("-") > 0:
            # self.zoom_min, self.zoom_max = map(int, zoom.split("-")) # Integer
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
        logging.info("image: " + self.image)
        logging.info("profile: " + self.profile)
        logging.info("alpha: " + str(self.alpha)) # alpha should be a tuple
        logging.info("timeout: " + str(self.timeout))
        logging.info("command: " + str(self.arglist))
   
    def mk_args(self):
        self.arglist = shlex.split(self.python_bin + " " + self.binary + 
                                   " --profile="+ self.profile + " -a " +
                                   self.alpha + " --zoom " + self.zoom_min+"-" 
                                   + self.zoom_max + " "+self.image )

    def __call__(self):
        """
        __call__ executes the class instance as a callable object
        """
        self.mk_args() # Prepare arguments

        print("Preparing arguments: ",self.arglist)
        self.process = subprocess.Popen(self.arglist, stdout=subprocess.PIPE) # Spawn process 

        step = 2.5
        total = 0.0
      
        def percent(x, a=0.8, b=0.2):
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
            # print(d," ",re.search(r'[^a-zA-Z0\s]',d))
            if not re.search(r'[^a-zA-Z0\s\-\:]',d):
                continue
            total += step
            print(d,"total", total, "pct", percent(total))

        print("Final total", total)

        print("Process spawned")

        try:
            outs, errs = self.process.communicate()
            print("I've got",outs)
        except TimeoutExpired:
            self.process.kill()
            outs, errs = self.process.communicate()
        
        print("Process spawned")

        spawned_processes[self.process.pid] = {"pid": self.process.pid,
                                               "image": self.image,
                                               "profile" : self.profile,
                                               "alpha" : str(self.alpha), # alpha should be a tuple
                                               "timeout" : self.timeout,
                                               "command":self.arglist}
       
        # Log the event
        self.mk_log()

        res = self.process.wait(timeout=self.timeout)
       
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

def main(image, profile, zoom, alpha, timeout=1800):
    G2T = GDAL2TilesSpawner(image, profile, zoom, alpha, timeout)

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
    parser.add_argument('-i', '--image', help="Path to the image where the plants will be counted.", type=str)
    parser.add_argument('-t', '--timeout', help="tolerance for the color detection.", type=float)
    parser.add_argument('-a', '--alpha', help="Circle specification [center_x,center_y,radious].", type=str)
    parser.add_argument('-z', '--zoom', help="Circle specification [center_x,center_y,radious].", type=str)
    parser.add_argument('-p', '--profile', help="Plant profile to be loaded")

    ## Parse the input arguments 
    args = parser.parse_args()

    # Check that the arguments passed in good shape
    if not None in [args.image, args.zoom, args.profile, args.alpha]:
        main(args.image,  args.profile, args.zoom, args.alpha)
    else:
        print("Wrong set of parameters")
