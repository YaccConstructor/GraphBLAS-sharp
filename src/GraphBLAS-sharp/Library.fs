namespace GraphBLAS_sharp

/// <summary> Initial module </summary>
module Say =

    open OpenCL.Net
    open Brahma.OpenCL
    open Brahma.FSharp.OpenCL.Core
    open Microsoft.FSharp.Quotations
    open Brahma.FSharp.OpenCL.Extensions

    let random = new System.Random()

    let MakeMatrix rows cols =
        Array.init (rows * cols) (fun i -> float32 (random.NextDouble()))

    let GetOutputMatrixDimensions aRows aCols bRows bCols =
        if aCols <> bRows
        then failwith "Cannot multiply these two matrices"
        aRows,bCols

    let Multiply (a:array<_>) aRows aCols (b:array<_>) bRows bCols (c:array<_>) =
        let cRows, cCols = GetOutputMatrixDimensions aRows aCols bRows bCols
        for i in 0 .. cRows - 1 do
            for j in 0 .. cCols - 1 do
                let mutable buf = 0.0f
                for k in 0 .. aCols - 1 do
                     buf <- buf + a.[i * aCols + k] * b.[k * bCols + j]
                c.[i * cCols + j] <- c.[i * cCols + j] + buf

    let Main platformName mSize =

        let m1 = (MakeMatrix mSize mSize)
        let m2 = (MakeMatrix mSize mSize)
        let localWorkSize = 2
        let iterations = 10
        let deviceType = DeviceType.Default

        let provider =
            try  ComputeProvider.Create(platformName, deviceType)
            with
            | ex -> failwith ex.Message

        let mutable commandQueue = new CommandQueue(provider, provider.Devices |> Seq.head)

        let aValues = m1
        let bValues = m2
        let cParallel = Array.zeroCreate(mSize * mSize)

        let command =
            <@
                fun (r:_2D) (a:array<_>) (b:array<_>) (c:array<_>) ->
                    let tx = r.GlobalID0
                    let ty = r.GlobalID1
                    let mutable buf = c.[ty * mSize + tx]
                    for k in 0 .. mSize - 1 do
                        buf <- buf + (a.[ty * mSize + k] * b.[k * mSize + tx])
                    c.[ty * mSize + tx] <- buf
            @>

        printfn "Multiplying two %Ax%A matrices %A times using .NET..." mSize mSize iterations
        let cNormal = Array.zeroCreate (mSize * mSize)
        let cpuStart = System.DateTime.Now
        for i in 0 .. iterations - 1 do
            Multiply aValues mSize mSize bValues mSize mSize cNormal
        let cpuTime = System.DateTime.Now - cpuStart

        printfn "done."

        printfn "Multiplying two %Ax%A matrices %A times using OpenCL and selected platform/device : %A ..." mSize mSize iterations provider

        let kernel, kernelPrepare, kernelRun = provider.Compile command
        let d =(new _2D(mSize, mSize, localWorkSize, localWorkSize))
        kernelPrepare d aValues bValues cParallel

        let gpuStart = System.DateTime.Now
        for i in 0 .. iterations - 1 do
            commandQueue.Add(kernelRun()).Finish() |> ignore
        let gpuTime = System.DateTime.Now - gpuStart

        let _ = commandQueue.Add(cParallel.ToHost provider).Finish()

        printfn "Verifying results..."
        let mutable isSuccess = true
        for i in 0 .. mSize * mSize - 1 do
            if isSuccess && System.Math.Abs(float32 (cParallel.[i] - cNormal.[i])) > 0.01f
            then
                isSuccess <- false
                printfn "Expected: %A Actual: %A Error = %A" cNormal.[i] cParallel.[i] (System.Math.Abs(cParallel.[i] - cNormal.[i]))

        printfn "done."

        cpuTime.TotalMilliseconds / float iterations |> printfn "Avg. time, F#: %A"
        gpuTime.TotalMilliseconds / float iterations |> printfn "Avg. time, OpenCL: %A"

        commandQueue.Dispose()
        provider.CloseAllBuffers()
        provider.Dispose()

