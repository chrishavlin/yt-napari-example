import argparse

import zarr.storage
# requires yt_experiments and zarr

import yt 
import numpy as np 
try: 
    from dask import delayed, compute
except ImportError:
    raise ImportError("you need dask[distributed]! pip install dask[distributed]")

try: 
    import zarr 
except ImportError:
    raise ImportError("you need zarr! pip install zarr")

try: 
    from yt_experiments.tiled_grid import YTTiledArbitraryGrid
except ImportError:
    raise ImportError("you need yt_experiments! pip install yt_experiments")

import os


def load_convert_single_timestep(enzo_64_dir: os.PathLike, 
                                 istep: int, 
                                 shp: tuple[int, int, int],
                                 chunksize: int, 
                                 top_level_store: zarr.storage.Store, 
                                 fieldtup: tuple[str, str],
                                 take_log=True): 
    
    
    yt.set_log_level(50)  # shhhhhh 

    istep_str = str(istep).zfill(4)
    file = os.path.join(enzo_64_dir, f"DD{istep_str}",f"data{istep_str}")

    try: 
        ds = yt.load(file)
    except FileNotFoundError:
        print("{file} not found, skipping")
        return


    print(f"timestep {istep_str} ideal resampling size is: {ds.domain_width / ds.index.get_smallest_dx()}")
    
    # create a tiled arbitrary grid (does not sample yet)
    tag = YTTiledArbitraryGrid(
        ds.domain_left_edge, 
        ds.domain_right_edge, 
        shp,  # desired size across all grids
        chunksize, # chunksize 
        ds=ds  # required for now
    )


    if istep_str in zarr_store:
        # remove it so we can re-run the script without error
        del zarr_store[istep_str]

    zarr_field = top_level_store.empty(istep_str, 
                    shape=tag.dims, 
                    chunks=tag.chunks, 
                    )
    
    # u add callback for loginess
    ops = []
    if take_log: 
        ops.append(np.log10)

    print(f"writing {fieldtup} to {istep_str} in {top_level_store}")
    _ = tag.to_array(
        fieldtup,
        output_array=zarr_field,
        ops=ops,
    )    



if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(
        prog="yt_ds_field_to_zarr",
        description="Convert a field from a yt dataset to a fixed res zarr file on disk",
    )
    
    parser.add_argument("-outfile", default="output.zarr", help="the output zarr")
    parser.add_argument("-sim_dir", default="Enzo_64", help="top-level simulation directory")
    parser.add_argument("-file_pattern", default="DD????/data????", help="file pattern for timesteps (default is that for Enzo_64)")
    parser.add_argument("-n_xyz", default="256,256,256", help="comma-separate list of dimensions for the output, default is 512,512,512")
    parser.add_argument("-chunksize", default=64, type=int, help="The chunksize to use (no partial chunks allowed!), default is 64")
    parser.add_argument("-field", default="gas,density", help="comma separated fieldtype, field name to sample")
    parser.add_argument("-take_log", type=bool, default=True, help="if true, take log10 by chunk")    
    parser.add_argument("-min_step", type=int, default=0, help="step to start at, default 0")    
    parser.add_argument("-max_step", type=int, default=43, help="step to end at, default 43")    


    args = parser.parse_args() 

    shp = tuple([int(n) for n in args.n_xyz.split(',')])
    fieldtup = tuple(args.field.split(","))
    
  # initialize a zarr store
    zarr_store = zarr.group(args.outfile)

    delayed_converts = []
    for istep in range(args.min_step, args.max_step+1):
        delayed_converts.append(
            delayed(load_convert_single_timestep)(args.sim_dir,
                                     istep, 
                                     shp, 
                                     args.chunksize,
                                     zarr_store,
                                     fieldtup,
                                     take_log=args.take_log
                                     )
        )

    compute(*delayed_converts)
  
    
