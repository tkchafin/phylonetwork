// Subworkflow to validate and filter VCF/BCF

include { BCFTOOLS_VIEW } from '../../../modules/nf-core/bcftools/view'
include { BCFTOOLS_INDEX as BCFTOOLS_INDEX1 } from '../../../modules/nf-core/bcftools/index'
include { BCFTOOLS_INDEX as BCFTOOLS_INDEX2 } from '../../../modules/nf-core/bcftools/index'
include { BCFTOOLS_QUERY as BCFTOOLS_QUERY_LIST } from '../../../modules/nf-core/bcftools/query'
include { VALIDATE_POP_MAP } from '../../../modules/local/validate_pop_map'


workflow validate_and_filter_vcf {

    take:
    input      // Channel with the path to the input VCF/BCF file
    pop_map    // Channel with the path to the population map file

    main:

    ch_versions = Channel.empty()

    // Step 1: Validate the pop_map file format
    VALIDATE_POP_MAP(
        pop_map
    )

    // Step 2: Index the input VCF file
    ch_vcf = input.map { vcf ->
        def baseName = vcf.getName().replaceAll(/(\.vcf\.gz|\.bcf\.gz|\.vcf|\.bcf)$/, '')
        tuple([id: baseName], vcf)
    }
    BCFTOOLS_INDEX1(
        ch_vcf
    )
    ch_versions = ch_versions.mix(BCFTOOLS_INDEX1.out.versions.first())

    // Step 3: Extract list of samples from input and compare with pop_map
    BCFTOOLS_QUERY_LIST(
        ch_vcf.join(BCFTOOLS_INDEX1.out.csi),
        [], [], []
    )
    ch_versions = ch_versions.mix(BCFTOOLS_QUERY_LIST.out.versions)

    // extract bcf samples
    ch_samples = BCFTOOLS_QUERY_LIST.out.output
        .map { tuple ->
            def file = tuple[1]
            file.text.readLines()
        }
        .flatten()
        .distinct()

    // extract pop_map samples
    ch_pop_samples = VALIDATE_POP_MAP.out.pop_map
        .splitCsv(header: false, sep: "\t")
        .map { row -> row[0] } // Extracting sample names from pop_map
        .distinct()

    // inner join
    ch_common_samples = ch_samples.join(ch_pop_samples).collect()

    // // Step 4: Filter the BCF to only the common samples
    ch_sample_file = ch_common_samples
        .map { commonSamples -> commonSamples.join('\n') }
        .collectFile(name: 'common_samples.txt')

    BCFTOOLS_VIEW(
        ch_vcf.join(BCFTOOLS_INDEX1.out.csi),
        [],[], ch_sample_file
    )
    ch_versions = ch_versions.mix(BCFTOOLS_VIEW.out.versions.first())
    ch_filtered = BCFTOOLS_VIEW.out.vcf.join(BCFTOOLS_VIEW.out.csi)

    // Step 5: Filter pop_map
    ch_filtered_pop_map = VALIDATE_POP_MAP.out.pop_map
        .splitCsv(header: false, sep: "\t")
        .filter { row -> ch_common_samples.map { it[0] }.toSortedList().contains(row[0]) }
        .map { row -> row.join("\t") }
        .collectFile(
            newLine: true,
            name: "${params.outdir}/validated_input/filtered_pop_map.txt"
        )

    emit:
    filtered_bcf     = BCFTOOLS_VIEW.out.vcf.join(BCFTOOLS_VIEW.out.csi)
    filtered_pop_map = ch_filtered_pop_map
    versions         = ch_versions
}
