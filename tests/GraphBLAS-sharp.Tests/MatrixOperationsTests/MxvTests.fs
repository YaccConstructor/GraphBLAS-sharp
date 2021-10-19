module Matrix.Mxv

open Expecto
open FsCheck
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Predefined
open TypeShape.Core
open Expecto.Logging
open Expecto.Logging.Message
open Brahma.FSharp.OpenCL
open OpenCL.Net

let logger = Log.create "Matrix.Mxv.Tests"

type OperationCase =
    { ClContext: ClContext
      MatrixCase: MatrixFromat
      VectorCase: VectorFormat
      MaskCase: MaskType }

let testCases =
    [ Utils.avaliableContexts "" |> Seq.map box
      Utils.listOfUnionCases<MatrixFromat>
      |> Seq.map box
      Utils.listOfUnionCases<VectorFormat>
      |> Seq.map box
      Utils.listOfUnionCases<MaskType> |> Seq.map box ]
    |> List.map List.ofSeq
    |> Utils.cartesian
    |> List.map
        (fun list ->
            { ClContext = unbox list.[0]
              MatrixCase = unbox list.[1]
              VectorCase = unbox list.[2]
              MaskCase = unbox list.[3] })

let correctnessGenericTest<'a when 'a: struct>
    (semiring: ISemiring<'a>)
    (isEqual: 'a -> 'a -> bool)
    (case: OperationCase)
    (matrix: 'a [,], vector: 'a [], mask: bool [])
    =

    let isZero = isEqual semiring.Zero

    let expected =
        let resultSize = Array2D.length1 matrix
        let resultVector = Array.zeroCreate<'a option> resultSize

        let times = semiring.Times.Invoke
        let plus = semiring.Plus.Invoke

        let task i =
            let col = matrix.[i, *]

            resultVector.[i] <-
                vector
                |> Array.Parallel.mapi
                    (fun i v ->
                        let res = times v col.[i]
                        if isZero res then None else Some res)
                |> Array.fold
                    (fun x y ->
                        match x, y with
                        | None, None -> None
                        | None, Some a -> Some a
                        | Some a, None -> Some a
                        | Some a, Some b -> Some <| plus a b)
                    None

        System.Threading.Tasks.Parallel.For(0, resultSize, task)
        |> ignore

        resultVector
        |> Seq.cast<'a option>
        |> Seq.mapi (fun i v -> (i, v))
        |> Seq.filter
            (fun (i, v) ->
                (not << Option.isNone) v
                && match case.MaskCase with
                   | NoMask -> true
                   | Regular -> mask.[i]
                   | Complemented -> not mask.[i])
        |> Seq.map (fun (i, v) -> i, Option.get v)
        |> Array.ofSeq
        |> Array.unzip
        |> fun (cols, vals) -> { Indices = cols; Values = vals }

    let actual =
        try
            let matrix =
                Utils.createMatrixFromArray2D case.MatrixCase matrix isZero

            let vector =
                Utils.createVectorFromArray case.VectorCase vector isZero

            let mask =
                Utils.createVectorFromArray VectorFormat.COO mask not

            logger.debug (
                eventX "Matrix is \n{matrix}"
                >> setField "matrix" matrix
            )

            logger.debug (
                eventX "Vector is \n{vector}"
                >> setField "vector" vector
            )

            graphblas {
                let! result =
                    match case.MaskCase with
                    | NoMask -> Matrix.mxv semiring matrix vector
                    | Regular -> failwith "fix me"
                    //Vector.mask mask
                    //>>= fun mask -> Matrix.mxvWithMask semiring mask matrix vector
                    | Complemented -> failwith "fix me"
                //Vector.complemented mask
                //>>= fun mask -> Matrix.mxvWithMask semiring mask matrix vector

                let! tuples = Vector.tuples result
                do! VectorTuples.synchronize tuples
                return tuples
            }
            |> EvalGB.withClContext case.ClContext
            |> EvalGB.runSync

        finally
            // TODO fix me
            ()
    // case.ClContext.Provider.CloseAllBuffers()

    logger.debug (
        eventX "Expected result is {expected}"
        >> setField "expected" (sprintf "%A" expected.Values)
    )

    logger.debug (
        eventX "Actual result is {actual}"
        >> setField "actual" (sprintf "%A" actual.Values)
    )

    let actualIndices = actual.Indices
    let expectedIndices = expected.Indices

    "Indices of expected and result vector must be the same"
    |> Expect.sequenceEqual actualIndices expectedIndices

    let equality =
        (expected.Values, actual.Values)
        ||> Seq.map2 isEqual

    "Length of expected and result values should be equal"
    |> Expect.hasLength actual.Values (Seq.length expected.Values)

    "There should be no difference between expected and received values"
    |> Expect.allEqual equality true

let testFixtures case =
    [ let config = Utils.defaultConfig

      let getCorrectnessTestName datatype =
          sprintf "Correctness on %s, %A" datatype case

      case
      |> correctnessGenericTest<int> AddMult.int (=)
      |> testPropertyWithConfig config (getCorrectnessTestName "int")

      case
      |> correctnessGenericTest<float> AddMult.float (fun x y -> abs (x - y) < Accuracy.low.relative)
      |> ptestPropertyWithConfig config (getCorrectnessTestName "float")

      case
      |> correctnessGenericTest<int16> AddMult.int16 (=)
      |> testPropertyWithConfig config (getCorrectnessTestName "int16")

      case
      |> correctnessGenericTest<uint16> AddMult.uint16 (=)
      |> testPropertyWithConfig config (getCorrectnessTestName "uint16")

      case
      |> correctnessGenericTest<bool> AnyAll.bool (=)
      |> ptestPropertyWithConfig config (getCorrectnessTestName "bool")

      testCase (sprintf "Explicit zero test on %A" case)
      <| fun () ->
          let matrix = array2D [ [ 1; 0 ]; [ 0; 0 ]; [ 1; 1 ] ]

          let vector = [| 4; -4 |]

          let expected =
              { Indices = [| 0; 2 |]
                Values = [| 4; 0 |] }

          let actual =
              try
                  let matrix =
                      Utils.createMatrixFromArray2D case.MatrixCase matrix ((=) 0)

                  let vector =
                      Utils.createVectorFromArray case.VectorCase vector ((=) 0)

                  graphblas { failwith "fix me" }
                  |> EvalGB.withClContext case.ClContext
                  |> EvalGB.runSync

              finally
                  // TODO fix me
                  failwith "fix me"
          // case.ClContext.Provider.CloseAllBuffers()
          failwith "fix me"
          //"Indices of actual and expected vectors should be the same"
          // |> Expect.sequenceEqual actual.Indices expected.Indices

          failwith "fix me"
      //"Values of actual and expected vectors should be the same"
      // |> Expect.sequenceEqual actual.Values expected.Values

      case
      |> correctnessGenericTest<CustomDatatypes.WrappedInt> CustomDatatypes.addMultSemiringOnWrappedInt (=)
      |> ptestPropertyWithConfig config (getCorrectnessTestName "WrappedInt") ]

let tests =
    testCases
    |> List.filter
        (fun case ->
            let mutable e = ErrorCode.Unknown
            let device = case.ClContext.Device

            let deviceType =
                Cl
                    .GetDeviceInfo(device, DeviceInfo.Type, &e)
                    .CastTo<DeviceType>()

            deviceType = DeviceType.Cpu
            && case.MatrixCase = CSR
            && case.VectorCase = VectorFormat.COO)
    |> List.collect testFixtures
    |> testList "Matrix.mxv tests"
