module Backend.SpMV

open Expecto
open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.ArraysExtensions
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Tests.Utils
open Microsoft.FSharp.Collections
open Microsoft.FSharp.Core
open OpenCL.Net
open Backend.Common.StandardOperations

let checkResult isEqual sumOp mulOp zero (baseMtx: 'a [,]) (baseVtr: 'b []) (actual: 'c array) =
    let rows = Array2D.length1 baseMtx
    let columns = Array2D.length2 baseMtx

    let expected = Array.create rows zero

    for i in 0 .. rows - 1 do
        let mutable sum = zero

        for v in 0 .. columns - 1 do
            sum <- sumOp sum (mulOp baseMtx.[i, v] baseVtr.[v])

        expected.[i] <- sum

    for i in 0 .. actual.Size - 1 do
        match actual.[i] with
        | Some v ->
            if isEqual zero v then
                failwith "Resulting zeroes should be implicit."
        | None -> ()

    for i in 0 .. actual.Size - 1 do
        match actual.[i] with
        | Some v ->
            Expect.isTrue (isEqual v expected.[i]) $"Values should be the same. Actual is {v}, expected {expected.[i]}."
        | None ->
            Expect.isTrue
                (isEqual zero expected.[i])
                $"Values should be the same. Actual is {zero}, expected {expected.[i]}."

let correctnessGenericTest
    zero
    sumOp
    mulOp
    (spMV: MailboxProcessor<_> -> Backend.CSRMatrix<'a> -> ClArray<'b option> -> ClArray<'c option>)
    (isEqual: 'a -> 'a -> bool)
    q
    (testContext: TestContext)
    (matrix: 'a [,], vector: 'a [], mask: bool [])
    =

    let mtx =
        createMatrixFromArray2D CSR matrix (isEqual zero)

    let vtr =
        createVectorFromArray Dense vector (isEqual zero)

    if mtx.NNZCount > 0 && vtr.Size > 0 then
        try
            let m = mtx.ToBackend testContext.ClContext

            match vtr, m with
            | VectorDense vtr, Backend.MatrixCSR m ->
                let v = vtr.ToDevice testContext.ClContext

                let res = spMV testContext.Queue m v

                (Backend.MatrixCSR m).Dispose q
                v.Dispose q
                let hostRes = res.ToHost q
                res.Dispose q

                checkResult isEqual sumOp mulOp zero matrix vector hostRes
            | _ -> failwith "Impossible"
        with
        | ex when ex.Message = "InvalidBufferSize" -> ()
        | ex -> raise ex

let testFixturesSpMV (testContext: TestContext) =
    [ let config = defaultConfig
      let wgSize = 32

      let getCorrectnessTestName datatype = sprintf "Correctness on %s" datatype

      let context = testContext.ClContext
      let q = testContext.Queue
      q.Error.Add(fun e -> failwithf "%A" e)

      let boolSpMV =
          Vector.spMV context boolSum boolMul wgSize

      testContext
      |> correctnessGenericTest false (||) (&&) boolSpMV (=) q
      |> testPropertyWithConfig config (getCorrectnessTestName "bool")

      let intSpMV = Vector.spMV context intSum intMul wgSize

      testContext
      |> correctnessGenericTest 0 (+) (*) intSpMV (=) q
      |> testPropertyWithConfig config (getCorrectnessTestName "int")

      let floatSpMV =
          Vector.spMV context floatSum floatMul wgSize

      testContext
      |> correctnessGenericTest 0.0 (+) (*) floatSpMV (fun x y -> abs (x - y) < Accuracy.medium.absolute) q
      |> testPropertyWithConfig config (getCorrectnessTestName "float")

      let byteAdd =
          Vector.spMV context byteSum byteMul wgSize

      let byteToCOO = Matrix.toCOO context wgSize

      testContext
      |> correctnessGenericTest 0uy (+) (*) byteAdd (=) q
      |> testPropertyWithConfig config (getCorrectnessTestName "byte") ]

let tests =
    availableContexts ""
    |> List.ofSeq
    |> List.filter
        (fun testContext ->
            let mutable e = ErrorCode.Unknown
            let device = testContext.ClContext.ClDevice.Device

            let deviceType =
                Cl
                    .GetDeviceInfo(device, DeviceInfo.Type, &e)
                    .CastTo<DeviceType>()

            deviceType = DeviceType.Gpu)
    |> List.distinctBy (fun testContext -> testContext.ClContext.ClDevice.DeviceType)
    |> List.collect testFixturesSpMV
    |> testList "Backend.Common.SpMV tests"
