/*
 *  Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.bwc;

import java.util.Map;
import lombok.SneakyThrows;
import static org.opensearch.neuralsearch.TestUtils.*;
import static org.opensearch.neuralsearch.TestUtils.objectToFloat;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;

public class TextSearchIT extends AbstractRestartUpgradeRestTestCase {

//   @Before
//   public void setUp() throws Exception {
//      super.setUp();
//      updateClusterSettings();
//      prepareModel();
//   }

   @SneakyThrows
   public void textSearch() {
      waitForClusterHealthGreen(NODES_BWC_CLUSTER);


      if(isRunningAgainstOldCluster()){
         updateClusterSettings();
         prepareModel();
         initializeBasicIndexIfNotExist(TEST_BASIC_INDEX_NAME);
//         String modelId = getDeployedModelId();
//
//         NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder(
//                 TEST_KNN_VECTOR_FIELD_NAME_1,
//                 TEST_QUERY_TEXT8,
//                 "",
//                 modelId,
//                 1,
//                 null,
//                 null
//         );

      }else {
         String modelId = getDeployedModelId();
         NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder(
                 TEST_KNN_VECTOR_FIELD_NAME_1,
                 TEST_QUERY_TEXT8,
                 "",
                 modelId,
                 1,
                 null,
                 null
         );
         Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, neuralQueryBuilder, 1);
         Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

         assertEquals("1", firstInnerHit.get("_id"));
         float expectedScore = computeExpectedScore(modelId, testVector, TEST_SPACE_TYPE, TEST_QUERY_TEXT8);
         assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), 0.0);
      }

   }



}