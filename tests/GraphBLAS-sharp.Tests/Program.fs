open Expecto
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Backend

// let matrixTests =
//     testList "Matrix" [ Matrix.Intersect.tests ]
//     |> testSequenced

// let commonTests =
//     let clArrayTests =
//         testList
//             "ClArray"
//             [ Common.ClArray.ExcludeElements.tests ]

    // testList
    //     "Common"
    //     [clArrayTests]
    // |> testSequenced
//
// let vectorTests =
//     testList
//         "Vector"
//         [ Vector.SpMV.tests
//           Vector.SpMSpV.tests
//           Vector.ZeroCreate.tests
//           Vector.OfList.tests
//           Vector.Copy.tests
//           Vector.Convert.tests
//           Vector.Map.allTests
//           Vector.Map2.allTests
//           Vector.AssignByMask.tests
//           Vector.AssignByMask.complementedTests
//           Vector.Reduce.tests
//           Vector.Merge.tests ]
//     |> testSequenced

// let algorithmsTests =
//     testList
//         "Algorithms tests"
//         [ Algorithms.MSBFS.levelsTests
//           Algorithms.MSBFS.parentsTests ]
//     |> testSequenced

// let deviceTests =
//     testList "Device" [ matrixTests ]
//     |> testSequenced

// let hostTests =
//     testList
//         "Host"
//         [ Host.Matrix.FromArray2D.tests
//           Host.Matrix.Convert.tests
//           Host.IO.MtxReader.test ]
//     |> testSequenced

[<Tests>]
let allTests =
    testList "All" [ Algorithms.MSBFS.levelsTests
                     Algorithms.MSBFS.parentsTests ] |> testSequenced

[<EntryPoint>]
let main argv = allTests |> runTestsWithCLIArgs [ CLIArguments.Allow_Duplicate_Names ] argv
