#!/bin/bash

echo "==========================================="
echo "Testing OpenSearch Forum Semantic Highlighting Issue"
echo "https://forum.opensearch.org/t/semantic-highlighting-results-not-great/26199"
echo "==========================================="
echo

echo "Test 1: Robotics Program Query"
echo "Expected: Should highlight text about robotics program"
echo "Forum reported: Incorrectly highlighted faith/Christian teachings instead"
echo "-------------------------------------------"
curl -X POST http://localhost:8080/predictions/semantic_highlighter \
  -H "Content-Type: application/json" \
  -d '{
    "inputs": [
      {
        "question": "Does this school have a robotics program?",
        "context": "Our school offers a comprehensive education rooted in Christian values. We have state-of-the-art facilities including science labs and computer rooms. The robotics program is one of our most popular extracurricular activities, winning multiple regional competitions. Students learn programming, engineering, and teamwork skills. Faith and academic excellence go hand in hand at our institution."
      }
    ]
  }' | jq '.'

echo
echo "Test 2: Autism Support Program Query"
echo "Expected: Should highlight text about autism support"
echo "Forum reported: Incorrectly highlighted other content"
echo "-------------------------------------------"
curl -X POST http://localhost:8080/predictions/semantic_highlighter \
  -H "Content-Type: application/json" \
  -d '{
    "inputs": [
      {
        "question": "does this school have a program for autism?",
        "context": "Welcome to our inclusive educational community. Christian teachings form the foundation of our curriculum. We pride ourselves on supporting diverse learners. Our specialized autism support program provides individualized instruction and therapeutic services. Trained specialists work closely with students on the autism spectrum. We believe every child deserves quality education in a nurturing environment."
      }
    ]
  }' | jq '.'

echo
echo "Test 3: Similar context structure - testing consistency"
echo "-------------------------------------------"
curl -X POST http://localhost:8080/predictions/semantic_highlighter \
  -H "Content-Type: application/json" \
  -d '{
    "inputs": [
      {
        "question": "What special programs does this school offer?",
        "context": "Our institution emphasizes traditional values and modern education. The campus features beautiful grounds and historic buildings. We offer specialized programs including advanced robotics, autism support services, and gifted education. Each program is staffed by certified specialists. Our commitment to excellence extends to all areas of student life."
      }
    ]
  }' | jq '.'

echo
echo "==========================================="
echo "Analysis Notes:"
echo "- The forum post indicates the model tends to highlight religious/faith content"
echo "- Even when specific programs (robotics, autism) are mentioned"
echo "- This suggests potential bias in the model training or weighting"
echo "==========================================="