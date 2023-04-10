module GraphBLAS.FSharp.Tests.Backend.Vector.SpMSpV

open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions
open Expecto
open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Context
open GraphBLAS.FSharp.Tests.TestCases
open Microsoft.FSharp.Collections
open Microsoft.FSharp.Core
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Vector
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Backend.Objects.ClContext

let config = Utils.defaultConfig

let wgSize = Utils.defaultWorkGroupSize

let checkResult isEqual sumOp mulOp zero (baseMtx: 'a [,]) (baseVtr: 'a []) (actual: int []) =
    let rows = Array2D.length2 baseMtx
    let columns = Array2D.length1 baseMtx

    let expectedV = Array.create rows zero
    let mutable expected = Array.create 0 0

    for i in 0 .. rows - 1 do
        let mutable sum = zero

        for v in 0 .. columns - 1 do
            sum <- sumOp sum (mulOp baseMtx.[v, i] baseVtr.[v])

        expectedV.[i] <- sum

    for i in 0 .. rows - 1 do
        if expectedV.[i] then
            expected <- Array.append expected [| i |]

    Expect.sequenceEqual actual expected $"Values should be the same. Actual is {actual}, expected {expected}."

let correctnessGenericTest
    zero
    sumOp
    mulOp
    (spMV: MailboxProcessor<_> -> ClMatrix.CSR<'a> -> ClVector.Sparse<'a> -> ClVector.Sparse<'a>)
    (isEqual: 'a -> 'a -> bool)
    q
    (testContext: TestContext)
    (vector: 'a [], matrix: 'a [,], _: bool [])
    =

    //Ensure that result is not empty
    vector.[0] <- true
    matrix.[0, 0] <- true

    let mtx =
        Utils.createMatrixFromArray2D CSR matrix (isEqual zero)

    let vtr =
        Utils.createVectorFromArray Sparse vector (isEqual zero)

    if mtx.NNZ > 0 && vtr.Size > 0 then
        try
            let m = mtx.ToDevice testContext.ClContext

            match vtr, m with
            | Vector.Sparse vtr, ClMatrix.CSR m ->
                let v = vtr.ToDevice testContext.ClContext

                let res = spMV testContext.Queue m v

                (ClMatrix.CSR m).Dispose q
                v.Dispose q
                let hostRes = res.Indices.ToHost q
                res.Dispose q

                checkResult isEqual sumOp mulOp zero matrix vector hostRes
            | _ -> failwith "Impossible"
        with
        | ex when ex.Message = "InvalidBufferSize" -> ()
        | ex -> raise ex

let createTest testContext (zero: 'a) isEqual add mul addQ mulQ =
    let context = testContext.ClContext
    let q = testContext.Queue

    let getCorrectnessTestName datatype =
        $"Correctness on %s{datatype}, %A{testContext.ClContext}"

    let spMSpV = SpMSpV.run context addQ mulQ wgSize

    testContext
    |> correctnessGenericTest zero add mul spMSpV isEqual q
    |> testPropertyWithConfig config (getCorrectnessTestName $"{typeof<'a>}")


let testFixturesSpMSpV (testContext: TestContext) =
    [ let context = testContext.ClContext
      let q = testContext.Queue
      q.Error.Add(fun e -> failwithf "%A" e)

      createTest testContext false (=) (||) (&&) ArithmeticOperations.boolSum ArithmeticOperations.boolMul ]

let tests =
    gpuTests "Backend.Vector.SpMSpV tests" testFixturesSpMSpV
