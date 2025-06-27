import argparse
# requires yt_experiments and zarr

import yt 
import numpy as np 

try: 
    import zarr 
except ImportError:
    raise ImportError("you need zarr! pip install zarr")

try: 
    from yt_experiments.tiled_grid import YTTiledArbitraryGrid
except ImportError:
    raise ImportError("you need yt_experiments! pip install yt_experiments")

import os


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(
        prog="yt_ds_field_to_zarr",
        description="Convert a field from a yt dataset to a fixed res zarr file on disk",
    )
    
    parser.add_argument("-outfile", default="output.zarr", help="the output zarr")
    parser.add_argument("-sample", default="Enzo_64", help="name of the yt sample dataset to load (needs to be a grid-based sample), default is Enzo_64")
    parser.add_argument("-n_xyz", default="512,512,512", help="comma-separate list of dimensions for the output, default is 512,512,512")
    parser.add_argument("-chunksize", default=64, type=int, help="The chunksize to use (no partial chunks allowed!), default is 64")
    parser.add_argument("-field", default="gas,density", help="comma separated fieldtype, field name to sample")
    parser.add_argument("-take_log", type=bool, default=True, help="if true, take log10 by chunk")    


    args = parser.parse_args() 

    shp = tuple([int(n) for n in args.n_xyz.split(',')])
    fieldtup = tuple(args.field.split(","))
    zarr_fieldname = "_".join(fieldtup)
    ds = yt.load_sample(args.sample)

    # create a tiled arbitrary grid (does not sample yet)
    tag = YTTiledArbitraryGrid(
        ds.domain_left_edge, 
        ds.domain_right_edge, 
        shp,  # desired size across all grids
        args.chunksize, # chunksize 
        ds=ds  # required for now
    )
    print(tag)

    # initialize a zarr store
    zarr_store = zarr.group(args.outfile)
    if zarr_fieldname in zarr_store:
        # remove it so we can re-run the script without error
        del zarr_store[zarr_fieldname]
    zarr_field = zarr_store.empty(zarr_fieldname, 
                                shape=tag.dims, 
                                chunks=tag.chunks, 
                                )

    # u add callback for loginess
    ops = []
    if args.take_log: 
        ops.append(np.log10)

    # actually do the thing
    print(f"writing {fieldtup} to {args.outfile}")
    _ = tag.to_array(
        fieldtup,
        output_array=zarr_field,
        ops=ops,
    )

    print("done! printing some of the resulting zarr directory structure:")
    print(os.listdir(os.path.join(zarr_store.store.path, "gas_density"))[:10])
