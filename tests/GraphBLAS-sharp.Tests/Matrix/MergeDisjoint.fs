module GraphBLAS.FSharp.Tests.Matrix.MergeDisjoint

open Expecto
open Expecto.Logging
open Brahma.FSharp
open GraphBLAS.FSharp.Test
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Context
open GraphBLAS.FSharp.Tests.TestCases
open GraphBLAS.FSharp.Backend.Matrix
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions

let config =
    { Utils.defaultConfig with
          endSize = 10
          maxTest = 10
          arbitrary = [ typeof<Generators.PairOfDisjointMatrices> ] }

let logger = Log.create "kronecker.Tests"

let workGroupSize = Utils.defaultWorkGroupSize

let makeTest context processor zero isEqual mergeFun (leftMatrix: 'a [,], rightMatrix: 'a [,]) =
    let m1 =
        Utils.createMatrixFromArray2D COO leftMatrix (isEqual zero)

    let m2 =
        Utils.createMatrixFromArray2D COO rightMatrix (isEqual zero)

    let expected =
        HostPrimitives.array2DMergeDisjoint leftMatrix rightMatrix zero isEqual

    let expected =
        match Utils.createMatrixFromArray2D COO expected (isEqual zero) with
        | Matrix.COO m -> m
        | _ -> failwith "incompatible format"

    let expected =
        expected.Rows, expected.Columns, expected.Values

    if m1.NNZ > 0 && m2.NNZ > 0 then
        let m1 =
            match m1 with
            | Matrix.COO m -> m.ToDevice context
            | _ -> failwith "Incompatible format"

        let m2 =
            match m2 with
            | Matrix.COO m -> m.ToDevice context
            | _ -> failwith "Incompatible format"

        let resultRows, resultCols, resultValues: ClArray<int> * ClArray<int> * ClArray<'a> =
            mergeFun processor m1.Rows m1.Columns m1.Values m2.Rows m2.Columns m2.Values

        let actual =
            (resultRows.ToHostAndFree processor,
             resultCols.ToHostAndFree processor,
             resultValues.ToHostAndFree processor)

        m1.Dispose processor
        m2.Dispose processor

        // Check result
        "Matrices should be equal"
        |> Expect.equal actual expected

let createGeneralTest (context: ClContext) (processor: MailboxProcessor<Msg>) (zero: 'a) isEqual =

    let merge =
        CSR.Kronecker.mergeDisjoint context workGroupSize

    makeTest context processor zero isEqual merge
    |> testPropertyWithConfig config $"test on %A{typeof<'a>}"

let generalTests (testContext: TestContext) =
    [ let context = testContext.ClContext
      let queue = testContext.Queue
      queue.Error.Add(fun e -> failwithf "%A" e)

      createGeneralTest context queue false (=)

      createGeneralTest context queue 0 (=)

      createGeneralTest context queue 0uy (=)

      createGeneralTest context queue 0.0f Utils.float32IsEqual

      if Utils.isFloat64Available context.ClDevice then
          createGeneralTest context queue 0.0 Utils.floatIsEqual ]

let tests =
    gpuTests "Backend.Matrix.MergeDisjoint tests" generalTests
