#!/bin/bash

echo "============================================="
echo "LOCALHOST MODEL PERFORMANCE ANALYSIS"
echo "============================================="
echo

# Test 1: Clear semantic match
echo "TEST SET 1: Clear Semantic Matches"
echo "-----------------------------------"
echo "1.1 Direct topic match:"
curl -s -X POST http://localhost:8080/predictions/semantic_highlighter \
  -H "Content-Type: application/json" \
  -d '{
    "inputs": [
      {
        "question": "What is the tuition cost?",
        "context": "Our school offers excellent education. The annual tuition is $15,000 per year. Financial aid is available for qualified families."
      }
    ]
  }' | jq -r '.highlights[0][] | "Highlighted: [\(.start):\(.end)]"' && echo

echo "1.2 Technical query match:"
curl -s -X POST http://localhost:8080/predictions/semantic_highlighter \
  -H "Content-Type: application/json" \
  -d '{
    "inputs": [
      {
        "question": "How do I install Python?",
        "context": "First, visit the Python website. Download the installer for your operating system. Run the installer and follow the prompts."
      }
    ]
  }' | jq -r '.highlights[0][] | "Highlighted: [\(.start):\(.end)]"' && echo

# Test 2: Ambiguous semantic relationships
echo "TEST SET 2: Ambiguous Semantic Relationships"
echo "--------------------------------------------"
echo "2.1 Indirect relationship:"
curl -s -X POST http://localhost:8080/predictions/semantic_highlighter \
  -H "Content-Type: application/json" \
  -d '{
    "inputs": [
      {
        "question": "Is the school environmentally friendly?",
        "context": "We have solar panels on every building. Students participate in recycling programs. The cafeteria sources food locally."
      }
    ]
  }' | jq -r '.highlights[0][] | "Highlighted: [\(.start):\(.end)]"' && echo

echo "2.2 Conceptual similarity:"
curl -s -X POST http://localhost:8080/predictions/semantic_highlighter \
  -H "Content-Type: application/json" \
  -d '{
    "inputs": [
      {
        "question": "What about student wellness?",
        "context": "Academic excellence is our priority. We have a full-time nurse and counselor. Students enjoy healthy lunch options daily."
      }
    ]
  }' | jq -r '.highlights[0][] | "Highlighted: [\(.start):\(.end)]"' && echo

# Test 3: No clear match scenarios
echo "TEST SET 3: No Clear Match Scenarios"
echo "------------------------------------"
echo "3.1 Unrelated content:"
curl -s -X POST http://localhost:8080/predictions/semantic_highlighter \
  -H "Content-Type: application/json" \
  -d '{
    "inputs": [
      {
        "question": "Do you offer music lessons?",
        "context": "Our math program is exceptional. Science labs are fully equipped. History classes include field trips to museums."
      }
    ]
  }' | jq -r 'if .highlights[0] | length > 0 then .highlights[0][] | "Highlighted: [\(.start):\(.end)]" else "No highlights" end' && echo

echo "3.2 Opposite meaning:"
curl -s -X POST http://localhost:8080/predictions/semantic_highlighter \
  -H "Content-Type: application/json" \
  -d '{
    "inputs": [
      {
        "question": "Is parking free?",
        "context": "Parking permits cost $200 per semester. Spaces are limited and assigned by lottery. Public transportation is recommended."
      }
    ]
  }' | jq -r 'if .highlights[0] | length > 0 then .highlights[0][] | "Highlighted: [\(.start):\(.end)]" else "No highlights" end' && echo

# Test 4: Multiple relevant sentences
echo "TEST SET 4: Multiple Relevant Sentences"
echo "---------------------------------------"
echo "4.1 Multiple matches:"
curl -s -X POST http://localhost:8080/predictions/semantic_highlighter \
  -H "Content-Type: application/json" \
  -d '{
    "inputs": [
      {
        "question": "Tell me about sports programs",
        "context": "We have varsity basketball and soccer teams. The swimming pool is Olympic-sized. Students can join intramural sports. Our teams won three championships last year."
      }
    ]
  }' | jq -r '.highlights[0][] | "Highlighted: [\(.start):\(.end)]"' && echo

# Test 5: Edge cases
echo "TEST SET 5: Edge Cases"
echo "----------------------"
echo "5.1 Very short context:"
curl -s -X POST http://localhost:8080/predictions/semantic_highlighter \
  -H "Content-Type: application/json" \
  -d '{
    "inputs": [
      {
        "question": "What is the school mascot?",
        "context": "We are the Eagles."
      }
    ]
  }' | jq -r 'if .highlights[0] | length > 0 then .highlights[0][] | "Highlighted: [\(.start):\(.end)]" else "No highlights" end' && echo

echo "5.2 Question words in context:"
curl -s -X POST http://localhost:8080/predictions/semantic_highlighter \
  -H "Content-Type: application/json" \
  -d '{
    "inputs": [
      {
        "question": "Where is the library?",
        "context": "Where students need resources, they find them. The library is on the second floor. It is open until 9 PM daily."
      }
    ]
  }' | jq -r '.highlights[0][] | "Highlighted: [\(.start):\(.end)]"' && echo

echo
echo "============================================="
echo "PERFORMANCE SUMMARY"
echo "============================================="