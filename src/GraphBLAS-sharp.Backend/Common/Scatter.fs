namespace GraphBLAS.FSharp.Backend.Common

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Quotes

module internal Scatter =
    let private general<'a> predicate (clContext: ClContext) workGroupSize =

        let run =
            <@ fun (ndRange: Range1D) (positions: ClArray<int>) (positionsLength: int) (values: ClArray<'a>) (result: ClArray<'a>) (resultLength: int) ->

                let gid = ndRange.GlobalID0

                if gid < positionsLength then
                    // positions lengths == values length
                    let predicateResult =
                        (%predicate) gid positionsLength positions

                    let position = positions.[gid]

                    if predicateResult
                       && 0 <= position
                       && position < resultLength then

                        result.[positions.[gid]] <- values.[gid] @>

        let program = clContext.Compile(run)

        fun (processor: MailboxProcessor<_>) (positions: ClArray<int>) (values: ClArray<'a>) (result: ClArray<'a>) ->

            if positions.Length <> values.Length then
                failwith "Lengths must be the same"

            let positionsLength = positions.Length

            let ndRange =
                Range1D.CreateValid(positionsLength, workGroupSize)

            let kernel = program.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.KernelFunc ndRange positions positionsLength values result result.Length)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    /// <summary>
    /// Creates a new array from the given one where it is indicated
    /// by the array of positions at which position in the new array
    /// should be a value from the given one.
    /// </summary>
    /// <remarks>
    /// Every element of the positions array must not be less than the previous one.
    /// If there are several elements with the same indices, the FIRST one of them will be at the common index.
    /// If index is out of bounds, the value will be ignored.
    /// </remarks>
    /// <example>
    /// <code>
    /// let positions = [| 0; 0; 1; 1; 1; 2; 3; 3; 4 |]
    /// let values = [| 1.9; 2.8; 3.7; 4.6; 5.5; 6.4; 7.3; 8.2; 9.1 |]
    /// run clContext 32 processor positions values result
    /// ...
    /// > val result = [| 1,9; 3.7; 6.4; 7.3; 9.1 |]
    /// </code>
    /// </example>
    let firstOccurrence clContext =
        general
        <| Predicates.firstOccurrence ()
        <| clContext

    /// <summary>
    /// Creates a new array from the given one where it is indicated by the array of positions at which position in the new array
    /// should be a value from the given one.
    /// </summary>
    /// <remarks>
    /// Every element of the positions array must not be less than the previous one.
    /// If there are several elements with the same indices, the LAST one of them will be at the common index.
    /// If index is out of bounds, the value will be ignored.
    /// </remarks>
    /// <example>
    /// <code>
    /// let positions = [| 0; 0; 1; 1; 1; 2; 3; 3; 4 |]
    /// let values = [| 1.9; 2.8; 3.7; 4.6; 5.5; 6.4; 7.3; 8.2; 9.1 |]
    /// run clContext 32 processor positions values result
    /// ...
    /// > val result = [| 2.8; 5.5; 6.4; 8.2; 9.1 |]
    /// </code>
    /// </example>
    let lastOccurrence clContext =
        general
        <| Predicates.lastOccurrence ()
        <| clContext

    let private generalInit<'a> predicate valueMap (clContext: ClContext) workGroupSize =

        let run =
            <@ fun (ndRange: Range1D) (positions: ClArray<int>) (positionsLength: int) (result: ClArray<'a>) (resultLength: int) ->

                let gid = ndRange.GlobalID0

                if gid < positionsLength then
                    // positions lengths == values length
                    let predicateResult =
                        (%predicate) gid positionsLength positions

                    let position = positions.[gid]

                    if predicateResult
                       && 0 <= position
                       && position < resultLength then

                        result.[positions.[gid]] <- (%valueMap) gid @>

        let program = clContext.Compile(run)

        fun (processor: MailboxProcessor<_>) (positions: ClArray<int>) (result: ClArray<'a>) ->

            let positionsLength = positions.Length

            let ndRange =
                Range1D.CreateValid(positionsLength, workGroupSize)

            let kernel = program.GetKernel()

            processor.Post(
                Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange positions positionsLength result result.Length)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    /// <summary>
    /// Creates a new array from the given one where it is indicated by the array of positions at which position in the new array
    /// should be a values obtained by applying the mapping to the global id.
    /// </summary>
    /// <remarks>
    /// Every element of the positions array must not be less than the previous one.
    /// If there are several elements with the same indices, the FIRST one of them will be at the common index.
    /// If index is out of bounds, the value will be ignored.
    /// </remarks>
    /// <example>
    /// <code>
    /// let positions = [| 0; 0; 1; 1; 1; 2; 3; 3; 4 |]
    /// let valueMap = id
    /// run clContext 32 processor positions values result
    /// ...
    /// > val result = [| 0; 2; 5; 6; 8 |]
    /// </code>
    /// </example>
    /// <param name="valueMap">Maps global id to a value</param>
    let initFirstOccurrence<'a> valueMap =
        generalInit<'a>
        <| Predicates.firstOccurrence ()
        <| valueMap

    /// <summary>
    /// Creates a new array from the given one where it is indicated by the array of positions at which position in the new array
    /// should be a values obtained by applying the mapping to the global id.
    /// </summary>
    /// <remarks>
    /// Every element of the positions array must not be less than the previous one.
    /// If there are several elements with the same indices, the LAST one of them will be at the common index.
    /// If index is out of bounds, the value will be ignored.
    /// </remarks>
    /// <example>
    /// <code>
    /// let positions = [| 0; 0; 1; 1; 1; 2; 3; 3; 4 |]
    /// let valueMap = id
    /// run clContext 32 processor positions values result
    /// ...
    /// > val result = [| 1; 4; 5; 7; 8 |]
    /// </code>
    /// </example>
    /// <param name="valueMap">Maps global id to a value</param>
    let initLastOccurrence<'a> valueMap =
        generalInit<'a>
        <| Predicates.lastOccurrence ()
        <| valueMap
