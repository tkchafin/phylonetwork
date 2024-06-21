process VALIDATE_POP_MAP {
    input:
    path pop_map

    output:
    path "validated_pop_map.txt", emit: pop_map

    script:
    """
    # Validate pop_map
    awk '
    BEGIN {
        OFS = "\\t"
    }
    {
        # Replace spaces or commas with tabs
        gsub(/[ ,]+/, "\\t", \$0)
        if (NF != 2) {
            print "Invalid format: each line must have exactly two columns" > "/dev/stderr"
            exit 1
        }
        orig_sample = \$1
        orig_population = \$2
        gsub(/[^a-zA-Z0-9_.-]/, "", \$1)
        gsub(/[^a-zA-Z0-9_.-]/, "", \$2)
        if (length(orig_sample) != length(\$1) || length(orig_population) != length(\$2)) {
            print "Invalid characters found in sample name:", orig_sample > "/dev/stderr"
            print "Invalid characters found in population name:", orig_population > "/dev/stderr"
            exit 1
        }
        print
    }' ${pop_map} > validated_pop_map.txt
    """
}
