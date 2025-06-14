We want to do this without using an Agent Pre-main class. Instead, we should apply these bytecode transformations at
build-time, so that the final shadowed JAR artifact just does the right thing without any runtime manipulation of
bytecode.
