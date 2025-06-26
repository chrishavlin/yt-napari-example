# yt-napari-example

Some self-contained examples of using yt-napari. Might be useful for demo purposes.

## installation 

In a fresh environment: 

```shell
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

If installing into an environment that already has napari and a qt backend, just install from `requirements_limited.txt`:

```shell
python -m pip install -r requirements_limited.txt
```

## examples

* `cosmological_network_Enzo64`: loads up a timestep from a cosmological simulation, plots the density field in napari using yt-napari.
