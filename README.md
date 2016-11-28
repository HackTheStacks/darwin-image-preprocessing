# darwin-image-preprocessing

Functions to get all darwin cut notes based on image dimensions and throw away full-page notes (non cut notes). Works by comparing image dimensions to mean image dimensions within folder. Written in PySpark for efficient parallel processing due to dataset size of ~350GB and ~60k images.
