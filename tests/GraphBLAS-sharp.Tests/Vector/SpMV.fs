module GraphBLAS.FSharp.Tests.Backend.Vector.SpMV

open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions
open Expecto
open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Tests.Utils
open GraphBLAS.FSharp.Tests.Context
open GraphBLAS.FSharp.Tests.TestCases
open Microsoft.FSharp.Collections
open Microsoft.FSharp.Core
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Vector
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Backend.Objects.ClContext

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
    (spMV: MailboxProcessor<_> -> AllocationFlag -> ClMatrix.CSR<'a> -> ClArray<'b option> -> ClArray<'c option>)
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
            let m = mtx.ToDevice testContext.ClContext

            match vtr, m with
            | Vector.Dense vtr, ClMatrix.CSR m ->
                let v = vtr.ToDevice testContext.ClContext

                let res = spMV testContext.Queue HostInterop m v

                (ClMatrix.CSR m).Dispose q
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

      let getCorrectnessTestName datatype =
          sprintf "Correctness on %s, %A" datatype testContext.ClContext

      let context = testContext.ClContext
      let q = testContext.Queue
      q.Error.Add(fun e -> failwithf "%A" e)

      let boolSpMV =
          SpMV.run context ArithmeticOperations.boolSum ArithmeticOperations.boolMul wgSize

      testContext
      |> correctnessGenericTest false (||) (&&) boolSpMV (=) q
      |> testPropertyWithConfig config (getCorrectnessTestName "bool")

      let intSpMV =
          SpMV.run context ArithmeticOperations.intSum ArithmeticOperations.intMul wgSize

      testContext
      |> correctnessGenericTest 0 (+) (*) intSpMV (=) q
      |> testPropertyWithConfig config (getCorrectnessTestName "int")

      let floatSpMV =
          SpMV.run context ArithmeticOperations.floatSum ArithmeticOperations.floatMul wgSize

      testContext
      |> correctnessGenericTest 0.0 (+) (*) floatSpMV (fun x y -> abs (x - y) < Accuracy.medium.absolute) q
      |> testPropertyWithConfig config (getCorrectnessTestName "float")

      let byteAdd =
          SpMV.run context ArithmeticOperations.byteSum ArithmeticOperations.byteMul wgSize

      testContext
      |> correctnessGenericTest 0uy (+) (*) byteAdd (=) q
      |> testPropertyWithConfig config (getCorrectnessTestName "byte") ]

let tests =
    gpuTests "Backend.Vector.SpMV tests" testFixturesSpMV
