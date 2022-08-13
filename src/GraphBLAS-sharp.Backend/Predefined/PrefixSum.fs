namespace GraphBLAS.FSharp.Backend.Predefined

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common

module internal PrefixSum =
    let standardExcludeInplace
        (clContext: ClContext)
        workGroupSize =

        let scan = PrefixSum.runExcludeInplace <@ (+) @> clContext workGroupSize

        fun (processor: MailboxProcessor<_>)
            (inputArray: ClArray<int>)
            (totalSum: ClArray<int>) ->

            scan
                processor
                inputArray
                totalSum
                0

    let standardIncludeInplace
        (clContext: ClContext)
        workGroupSize =

        let scan = PrefixSum.runIncludeInplace <@ (+) @> clContext workGroupSize

        fun (processor: MailboxProcessor<_>)
            (inputArray: ClArray<int>)
            (totalSum: ClArray<int>) ->

            scan
                processor
                inputArray
                totalSum
                0

    let standardInclude
        (clContext: ClContext)
        workGroupSize =

        let scan = PrefixSum.runInclude <@ (+) @> clContext workGroupSize

        fun (processor: MailboxProcessor<_>)
            (inputArray: ClArray<int>)
            (totalSum: ClArray<int>) ->

            scan
                processor
                inputArray
                totalSum
                0

    let standardExclude
        (clContext: ClContext)
        workGroupSize =

        let scan = PrefixSum.runExclude <@ (+) @> clContext workGroupSize

        fun (processor: MailboxProcessor<_>)
            (inputArray: ClArray<int>)
            (totalSum: ClArray<int>) ->

            scan
                processor
                inputArray
                totalSum
                0
