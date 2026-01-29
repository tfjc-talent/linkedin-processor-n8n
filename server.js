// server.js
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const compression = require('compression');
const cluster = require('cluster');
const os = require('os');

// Clustering for parallel processing
if (cluster.isMaster && process.env.NODE_ENV === 'production') {
  const numWorkers = Math.min(4, os.cpus().length); // Max 4 workers for Railway
  
  console.log(`🚀 Master process ${process.pid} starting ${numWorkers} workers`);
  
  for (let i = 0; i < numWorkers; i++) {
    cluster.fork();
  }
  
  cluster.on('exit', (worker, code, signal) => {
    console.log(`⚠️  Worker ${worker.process.pid} died. Restarting...`);
    cluster.fork();
  });
  
  // Don't continue with Express setup in master process
  return;
}

// Worker process continues with Express setup
const app = express();

// Security and optimization middleware
app.use(helmet());
app.use(compression());
app.use(cors());
app.use(express.json({ limit: '100mb' }));

// EXACT COPY OF YOUR FUNCTION - NO CHANGES
function processProfile(profile) {
  const output = {};

  // Collect keywords from specific properties
  const keywordsSet = new Set();

  // Extract from summary
  if (profile.summary && profile.summary != "") {
    profile.summary.split(/\s+/).forEach(word => keywordsSet.add(word));
  }

  // store old url
  output.linkedin_public_identifier = profile.linkedin_public_identifier;
  
  // Extract from headline
  if (profile.headline) {
    profile.headline.split(/\s+/).forEach(word => keywordsSet.add(word));
  }

  // Extract from skills (array of objects with 'name' property)
  if (Array.isArray(profile.skills)) {
    profile.skills.forEach(skill => {
      if (skill && skill.name) {
        skill.name.split(/\s+/).forEach(word => keywordsSet.add(word));
      }
    });
  }

  // Extract from languages (array of objects with 'name' property)
  if (Array.isArray(profile.languages)) {
    profile.languages.forEach(language => {
      if (language && language.name) {
        language.name.split(/\s+/).forEach(word => keywordsSet.add(word));
      }
    });
  }

  // Process positions and build 'experience'
  const positions = profile.position || [];
  const experienceArray = [];

  let lastCompanyId = null;
  let currentExperienceEntry = null;

  positions.forEach(pos => {
    // Extract company and job information
    const companyName = pos.companyName || 'Unknown Company';
    const companyId = pos.companyId || '';
    const companyLink = pos.companyURL || '';
    const companyLogo = pos.companyLogo || '';
    const companyRange = pos.companyStaffCountRange || '';
    const companyIndustry = pos.companyIndustry || '';
    const companyDescription = pos.companyDescription || '';
    const companySpecialties = pos.companySpecialties || '';

    // Calculate job duration and date range
    const now = new Date();

    const startDate = createDateFromPositionDate(pos.start);
    const endDate = pos.end && createDateFromPositionDate(pos.end) || now;

    let jobDurationMonths = 0;
    let dateRange = '';
    if (startDate) {
      jobDurationMonths = Math.max(0, (endDate - startDate) / (1000 * 60 * 60 * 24 * 30));
      // Format date range
      const options = { year: 'numeric', month: 'short' };
      const startDateStr = startDate.toLocaleDateString('en-US', options);
      const endDateStr = endDate < now ? endDate.toLocaleDateString('en-US', options) : 'Present';
      dateRange = `${startDateStr} - ${endDateStr}`;
    }

    // Build job entry
    const jobEntry = {
      jobTitle: pos.title || '',
      duration: Math.round(jobDurationMonths),
      dateRange: dateRange,
      description: pos.description || '',
      employmentType: pos.employmentType || '',
    };

    // Extract keywords from job title and description
    if (pos.title) {
      pos.title.split(/\s+/).forEach(word => keywordsSet.add(word));
    }
    if (pos.description) {
      pos.description.split(/\s+/).forEach(word => keywordsSet.add(word));
    }

    // Check if the current position is at the same company as the last position
    if (companyId === lastCompanyId && currentExperienceEntry) {
      // Add job to the current company entry
      currentExperienceEntry.jobs.push(jobEntry);
    } else {
      // Create a new company entry
      currentExperienceEntry = {
        company: companyName,
        companyId: companyId,
        companyLink: companyLink,
        companyLogo: companyLogo,
        companyRange: companyRange,
        companyIndustry: companyIndustry,
        companyDescription: companyDescription,
        companySpecialties: companySpecialties,
        jobs: [jobEntry],
      };
      experienceArray.push(currentExperienceEntry);
    }

    // Update lastCompanyId
    lastCompanyId = companyId;
  });

  // Assign the experience array to the output
  output.experiences = experienceArray;

  // Calculate years of experience based on relevant positions
  // First, filter out internships and volunteering positions
  const relevantPositions = positions.filter(pos => !isExcludedPosition(pos));

  // Find positions with at least 7 months duration and get the earliest start date
  let earliestRelevantStartDate = null;
  const now = new Date(); // Current date

  relevantPositions.forEach(pos => {
    const startDate = createDateFromPositionDate(pos.start);
    const endDate = pos.end && createDateFromPositionDate(pos.end) || now;

    if (!startDate) {
      // Cannot calculate duration without start date
      return; // Skip this position
    }

    const jobDurationMonths = Math.max(0, (endDate - startDate) / (1000 * 60 * 60 * 24 * 30));

    if (jobDurationMonths >= 7) {
      if (!earliestRelevantStartDate || startDate < earliestRelevantStartDate) {
        earliestRelevantStartDate = startDate;
      }
    }
  });

  if (earliestRelevantStartDate) {
    const experienceYears = (now - earliestRelevantStartDate) / (1000 * 60 * 60 * 24 * 365.25);
    output.years_of_experience = Math.round(experienceYears);
  } else {
    // If we cannot calculate years of experience, output 99
    output.years_of_experience = 99;
  }
  output.keywords = Array.from(keywordsSet).join(' ');

  // Get current position (assuming the first position without an end date is the current one)
  const currentPosition = positions.find(pos => {
    const end = pos.end;
    return !end || !end.year; // No end date implies current position
  });

  if (currentPosition) {
    output.current_industry = currentPosition.companyIndustry || '';
    output.current_job_title = currentPosition.title || '';
    output.current_company = currentPosition.companyName || '';
    output.current_employment_type = currentPosition.employmentType || '';
    output.current_company_employee_range = currentPosition.companyStaffCountRange || '';
  } else {
    output.current_industry = '';
    output.current_job_title = '';
    output.current_company = '';
    output.current_employment_type = '';
    output.current_company_employee_range = '';
  }

  // Collect all job titles
  const jobTitles = relevantPositions.map(pos => pos.title).filter(Boolean);
  output.job_titles = jobTitles.join(', ');

  // Collect all industries
  const industries = relevantPositions.map(pos => pos.companyIndustry).filter(Boolean);
  output.industries_tag = industries.join(', ');

  // create new educations array
  const educationsArray = [];
  if (Array.isArray(profile.educations)) {
    profile.educations.forEach(edu => {
      const educationEntry = {};

      // Format the date
      let startYear = edu.start && edu.start.year ? edu.start.year : null;
      let endYear = edu.end && edu.end.year ? edu.end.year : null;
      let dateStr = '';
      if (startYear && endYear) {
        dateStr = `${startYear} - ${endYear}`;
      } else if (startYear) {
        dateStr = `Depuis ${startYear}`;
      } else if (endYear) {
        dateStr = `Jusqu'à ${endYear}`;
      }

      if (dateStr) {
        educationEntry.date = dateStr;
      }

      // Construct the degree string
      let degree = edu.degree || '';
      let fieldOfStudy = edu.fieldOfStudy || '';
      let degreeStr = '';
      if (degree && fieldOfStudy) {
        degreeStr = `${degree}, ${fieldOfStudy}`;
      } else if (degree) {
        degreeStr = degree;
      } else if (fieldOfStudy) {
        degreeStr = fieldOfStudy;
      }

      if (degreeStr) {
        educationEntry.degree = degreeStr;
      }

      // School name
      educationEntry.school = edu.schoolName || '';

      // Add the education entry to the array if it has at least degree or school
      if (educationEntry.degree || educationEntry.school) {
        educationsArray.push(educationEntry);
      }
    });
  }
  output.educations = educationsArray;
  
  // Add the additional fields
  output.firstname = profile.firstName || '';
  output.lastname = profile.lastName || '';
  output.profile_pic = profile.profilePicture || '';
  output.spotlight = profile.isOpenToWork ? "Open to work" : "" || "";
  output.is_open_to_work = profile.isOpenToWork ?? false
  output.is_hiring = profile.isHiring ?? false
  output.about = profile.summary || '';
  output.languages = (() => {
    // Check if profile.languages exists and is not empty
    if (profile.languages && profile.languages.length > 0) {
      let languages = profile.languages.map(el => el.name);

      // Check if supportedLocales[0].country is "FR" and "Français" or "French" is not in the languages
      if (profile.supportedLocales[0]?.country === "FR" && !languages.some(lang => ["Français", "French"].includes(lang))) {
        languages.push("Français");
      }

      // Check if supportedLocales[0].country is "US" or "EN" and "English" is not in the languages
      if (["US", "EN"].includes(profile.supportedLocales[0]?.country) && !languages.some(lang => ["English"].includes(lang))) {
        languages.push("English");
      }

      return languages.join(', ');
    }

    // If profile.languages is null or empty, fallback to supportedLocales
    if (profile.languages == null || profile.languages.length === 0) {
      if (profile.supportedLocales[0]?.country === "FR") {
        return "Français";
      } else if (["US", "EN"].includes(profile.supportedLocales[0]?.country)) {
        return "English";
      }
    }

    // Default empty string if no languages or valid fallback found
    return "";
  })();
  output.skills = profile.skills != null ? {skills:profile.skills.map(el => el.name),details:profile.skills} : {};
  output.headline = profile.headline || '';
  output.current_employment_duration = output.experiences?.[0]?.jobs?.[0]?.duration || 0;

  // handle education which are often empty
  if (output.educations.length > 0) {
    output.degrees = output.educations?.map(el => el.degree).join(', ') || '';
    output.schools = output.educations?.map(el => el.school).join(', ') || '';
  } else if (output.educations.length == 0) {
    output.degrees = '';
    output.schools = '';
  }
  
  output.companies = output.experiences?.map(el => el.company).join(',') || '';
  
  output.public_linkedin_identifier = decodeURI(profile.username) || '';
  output.linkedin_url = decodeURI("https://linkedin.com/in/"+profile.username) || '';
  output.urn = profile.urn;

  // Construct location field (city, region, country in one field)
  if (profile.geo && profile.geo.full) {
    output.location = profile.geo.full;
  } else {
    output.location = '';
  }

  // Include the raw input profile data
  output.profil_details = profile;

  return output;

  // Helper function to create date from position date, handling missing months and days
  function createDateFromPositionDate(positionDate) {
    if (!positionDate || !positionDate.year) {
      return null;
    }
    let month = positionDate.month;
    if (!month || month === 0) {
      // If month is missing or 0, set it to July (mid-year)
      month = 6; // July (zero-based)
    } else {
      month = month - 1; // Adjust to zero-based
    }
    let day = positionDate.day;
    if (!day || day === 0) {
      day = 1;
    }
    return new Date(positionDate.year, month, day);
  }

  // Helper function to determine if a position should be excluded
  function isExcludedPosition(pos) {
    const employmentType = pos.employmentType ? pos.employmentType.toLowerCase() : '';
    const title = pos.title ? pos.title.toLowerCase() : '';

    return employmentType.includes('intern') ||
      title.includes('stagiaire') ||
      employmentType.includes('volunteer') ||
      title.includes('bénévole');
  }
}

// API Routes

// Health check endpoint
app.get('/health', (req, res) => {
  const usage = process.memoryUsage();
  res.json({
    status: 'healthy',
    memory: {
      rss: `${Math.round(usage.rss / 1024 / 1024)}MB`,
      heapUsed: `${Math.round(usage.heapUsed / 1024 / 1024)}MB`,
      heapTotal: `${Math.round(usage.heapTotal / 1024 / 1024)}MB`
    },
    uptime: Math.round(process.uptime()),
    timestamp: new Date().toISOString()
  });
});

// Process profiles endpoint - EXACTLY like your n8n Code node
app.post('/api/process-profiles', (req, res) => {
  const startTime = Date.now();
  
  try {
    // Get the data - handle both single object and array formats
    let items = req.body;
    
    // If it's a single object, wrap it in an array
    if (!Array.isArray(items)) {
      if (items && typeof items === 'object') {
        items = [items];
        console.log('📥 Converted single object to array');
      } else {
        return res.status(400).json({
          error: 'Invalid input format. Expected object or array.',
          received: typeof items
        });
      }
    }
    
    console.log('📥 Received data type:', typeof req.body);
    console.log('📥 Is array?', Array.isArray(items));
    console.log('📥 Items count:', items.length);
    
    // Validate input
    if (!Array.isArray(items)) {
      return res.status(400).json({
        error: 'Invalid input after processing. Contact support.',
        received: typeof items
      });
    }

    // Debug first item structure
    if (items.length > 0) {
      console.log('📥 First item keys:', Object.keys(items[0]));
      console.log('📥 First item has json?', items[0].hasOwnProperty('json'));
      if (items[0].json) {
        console.log('📥 First item.json has urn?', items[0].json.hasOwnProperty('urn'));
      }
    }

    // Handle both formats: direct profiles OR n8n wrapped format
    let filteredItems;
    let profiles;
    
    // Check if data is in n8n format {json: profile} or direct profile format
    if (items[0] && items[0].json) {
      // n8n format: [{json: profile}, {json: profile}]
      filteredItems = items.filter(el => el.json && el.json.hasOwnProperty('urn'));
      profiles = filteredItems.map(item => item.json);
      console.log('🔍 Using n8n format - Filtered items count:', filteredItems.length);
    } else {
      // Direct profile format: [profile, profile]
      filteredItems = items.filter(el => el.hasOwnProperty('urn'));
      profiles = filteredItems;
      console.log('🔍 Using direct format - Filtered items count:', filteredItems.length);
    }
    
    const outputItems = [];

    // Process each profile - EXACT same logic
    for (let i = 0; i < profiles.length; i++) {
      const profile = profiles[i];

      console.log(`⚙️  Processing profile ${i + 1}: ${profile.firstName} ${profile.lastName}`);

      // Process the profile using your exact function
      const processedProfile = processProfile(profile);

      // Add the processed profile to the output - DIRECT FORMAT (no json wrapper)
      outputItems.push(processedProfile);

      // Force garbage collection every 10 profiles
      if (i % 10 === 0 && global.gc) {
        global.gc();
      }
    }

    const processingTime = Date.now() - startTime;

    console.log('✅ Processing complete:', {
      processed: outputItems.length,
      filtered: profiles.length,
      total: items.length,
      processingTimeMs: processingTime
    });

    // Return direct array of processed profiles (no json wrapper)
    res.json({
      items: outputItems,
      metadata: {
        processed: outputItems.length,
        filtered: profiles.length,
        total: items.length,
        processingTimeMs: processingTime
      }
    });

    // Force cleanup after response
    setImmediate(() => {
      if (global.gc) {
        global.gc();
      }
    });

  } catch (error) {
    console.error('❌ Processing error:', error);
    res.status(500).json({
      error: 'Processing failed',
      message: error.message,
      stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
    });
  }
});

// Test endpoint for local development
app.post('/api/test', (req, res) => {
  try {
    const testData = require('./test-data.json');
    
    // Handle both formats like main endpoint
    let profiles;
    if (testData[0] && testData[0].json) {
      const filteredItems = testData.filter(el => el.json && el.json.hasOwnProperty('urn'));
      profiles = filteredItems.map(item => item.json);
    } else {
      profiles = testData.filter(el => el.hasOwnProperty('urn'));
    }

    const outputItems = [];
    for (let i = 0; i < profiles.length; i++) {
      const profile = profiles[i];
      const processedProfile = processProfile(profile);
      outputItems.push(processedProfile); // Direct format, no json wrapper
    }

    res.json({
      message: 'Test completed successfully',
      items: outputItems,
      processed: outputItems.length
    });

  } catch (error) {
    res.status(500).json({
      error: 'Test failed',
      message: error.message
    });
  }
});

app.get('/debug', (req, res) => {
  res.json({
    nodeEnv: process.env.NODE_ENV,
    isMaster: cluster.isMaster,
    isWorker: cluster.isWorker,
    processId: process.pid,
    workerId: cluster.worker?.id || 'master'
  });
});

// Error handling middleware
app.use((error, req, res, next) => {
  console.error('Unhandled error:', error);
  res.status(500).json({
    error: 'Internal server error',
    message: process.env.NODE_ENV === 'development' ? error.message : 'Something went wrong'
  });
});

// 404 handler
app.use((req, res) => {
  res.status(404).json({
    error: 'Not found',
    message: `Route ${req.method} ${req.path} not found`
  });
});

const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  const workerInfo = cluster.isWorker ? ` (Worker ${process.pid})` : '';
  console.log(`🚀 LinkedIn Processor API${workerInfo} running on port ${PORT}`);
  console.log(`📊 Health check: http://localhost:${PORT}/health`);
  console.log(`🧪 Test endpoint: http://localhost:${PORT}/api/test`);
  console.log(`⚡ Process endpoint: http://localhost:${PORT}/api/process-profiles`);
  
  // Memory optimization settings
  if (global.gc) {
    console.log('✅ Garbage collection enabled');
    global.gc();
  }
});

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('🛑 SIGTERM received, shutting down gracefully');
  process.exit(0);
});

process.on('SIGINT', () => {
  console.log('🛑 SIGINT received, shutting down gracefully');
  process.exit(0);
});