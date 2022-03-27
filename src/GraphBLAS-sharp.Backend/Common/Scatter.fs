namespace GraphBLAS.FSharp.Backend.Common

open Brahma.FSharp.OpenCL
open Microsoft.FSharp.Quotations

module internal Scatter =
    let private runInPlace
        getter
        (clContext: ClContext)
        workGroupSize =

        let run =
            <@
                fun (ndRange: Range1D)
                    (positions: ClArray<int>)
                    (positionsLength: int)
                    (values: ClArray<'a>)
                    (result: ClArray<'a>)
                    (resultLength: int) ->

                    let i = ndRange.GlobalID0

                    if i < positionsLength then
                        let index = positions.[i]
                        if 0 <= index && index < resultLength then
                            if i < positionsLength - 1 then
                                if index <> positions.[i + 1] then
                                    result.[index] <- (%getter) values i
                            else
                                result.[index] <- (%getter) values i
            @>
        let kernel = clContext.CreateClProgram(run).GetKernel()

        fun (processor: MailboxProcessor<_>)
            (positions: ClArray<int>)
            (values: ClArray<'a>)
            (result: ClArray<'a>) ->

            let positionsLength = positions.Length

            let ndRange = Range1D.CreateValid(positionsLength, workGroupSize)

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            positions
                            positionsLength
                            values
                            result
                            result.Length)
            )
            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

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
    /// let ps = [| 0; 0; 1; 1; 1; 2; 3; 3; 4 |]
    /// let arr = [| 1.9; 2.8; 3.7; 4.6; 5.5; 6.4; 7.3; 8.2; 9.1 |]
    /// let res = run clContext 64 processor ps arr 5
    /// ...
    /// > val res = [| 2.8; 5.5; 6.4; 8.2; 9.1 |]
    /// </code>
    /// </example>
    /// <returns>
    ///
    /// </returns>
    let arrayInPlace clContext =
        runInPlace
            <@
                fun (values: ClArray<'a>)
                    (i: int) ->
                    values.[i]
            @>
            clContext

    let initInPlace
        (initializer: Expr<int -> 'a>)
        clContext
        workGroupSize =

        let runInPlace =
            runInPlace
                <@
                    fun (_: ClArray<'a>)
                        (i: int) ->
                        (%initializer) i
                @>
                clContext
                workGroupSize

        fun (processor: MailboxProcessor<_>)
            (positions: ClArray<int>)
            (result: ClArray<'a>) ->

            let a = clContext.CreateClArray(1)
            runInPlace processor positions a result
            processor.Post(Msg.CreateFreeMsg(a))

    let constInPlace
        constant
        clContext
        workGroupSize =

        let initInPlace =
            initInPlace
                <@ fun _ -> constant @>
                clContext
                workGroupSize

        fun (processor: MailboxProcessor<_>)
            (positions: ClArray<int>)
            (result: ClArray<'a>) ->

            initInPlace processor positions result
