namespace GraphBLAS.FSharp.Backend.Common

open Brahma.FSharp

module internal Scatter =

    /// <summary>
    /// Creates a new array from the given one where it is indicated by the array of positions at which position in the new array
    /// should be a value from the given one.
    /// </summary>
    /// <remarks>
    /// Every element of the positions array must not be less than the previous one.
    /// If there are several elements with the same indices, the last one of them will be at the common index.
    /// If index is out of bounds, the value will be ignored.
    /// </remarks>
    /// <example>
    /// <code>
    /// let positions = [| 0; 0; 1; 1; 1; 2; 3; 3; 4 |]
    /// let values = [| 1.9; 2.8; 3.7; 4.6; 5.5; 6.4; 7.3; 8.2; 9.1 |]
    /// let result = run clContext 32 processor positions values result
    /// ...
    /// > val result = [| 2.8; 5.5; 6.4; 8.2; 9.1 |]
    /// </code>
    /// </example>
    let runInplace<'a> (clContext: ClContext) workGroupSize =

        let run =
            <@ fun (ndRange: Range1D) (positions: ClArray<int>) (positionsLength: int) (values: ClArray<'a>) (result: ClArray<'a>) (resultLength: int) ->

                let i = ndRange.GlobalID0

                if i < positionsLength then
                    let index = positions.[i]

                    if 0 <= index && index < resultLength then
                        if i < positionsLength - 1 then
                            if index <> positions.[i + 1] then
                                result.[index] <- values.[i]
                        else
                            result.[index] <- values.[i] @>

        let program = clContext.Compile(run)

        fun (processor: MailboxProcessor<_>) (positions: ClArray<int>) (values: ClArray<'a>) (result: ClArray<'a>) ->

            let positionsLength = positions.Length

            let ndRange =
                Range1D.CreateValid(positionsLength, workGroupSize)

            let kernel = program.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.KernelFunc ndRange positions positionsLength values result result.Length)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))
